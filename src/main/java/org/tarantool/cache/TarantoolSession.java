/**
 * Copyright 2018 Evgeniy Zaikin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tarantool.cache;

import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClientOps;
import org.tarantool.xml.XmlConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

/**
 * The implementation of the {@link TarantoolSession}.
 * TarantoolSession associated with one Tarantool instance.
 * URI for each TarantoolSession should be unique.
 *
 * If password and(or) user is empty, defaults would be taken.
 * If host is empty, 'localhost' would be taken.
 * If port is empty, default Tarantool port 3301 would be taken.
 *
 * @author Evgeniy Zaikin
 */
public class TarantoolSession implements Closeable {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3301;
    private final TarantoolClientImpl tarantoolClient;
    private final XmlConfiguration xmlConfiguration;

    private URL resolveResourcePath(URI uri, ClassLoader classLoader) {
        if (uri.getScheme().equals("classpath")) {
            return classLoader.getResource(uri.getSchemeSpecificPart());
        }
        try {
            return uri.toURL();
        } catch (MalformedURLException e) {
            // wrap MalformedURLException into TarantoolCacheException
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Constructs a new TarantoolSession.
     *
     * @param uri             the resource
     * @param classLoader     the classLoader will be used for loading classes
     * @throws NullPointerException if the URI and/or classLoader is null.
     * @throws IllegalArgumentException if connection parameters obtained from XML are incorrect or if incorrect URI passed
     * @throws TarantoolCacheException if anything went wrong parsing the XML
     */
    public TarantoolSession(URI uri, ClassLoader classLoader) {
        if (uri == null || classLoader == null) {
            throw new NullPointerException("No TarantoolSession URI specified");
        }

        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        TarantoolClientConfig config = new TarantoolClientConfig();
        if (uri.getScheme() != null) {
            if (uri.getScheme().equals("file") || uri.getScheme().equals("classpath")) {
                URL url = resolveResourcePath(uri, classLoader);
                try {
                    xmlConfiguration = new XmlConfiguration(url, classLoader);
                } catch (RuntimeException e) {
                    throw new TarantoolCacheException(e);
                }
            } else if (uri.getScheme().equals("http") || uri.getScheme().equals("tarantool")) {
                /* Example: "tarantool://user:password@host:port" */
                if (uri.getHost() == null) {
                    throw new IllegalArgumentException("Invalid host given by URI");
                }
                host = uri.getHost();
                port = uri.getPort() != -1 ? uri.getPort() : DEFAULT_PORT;
                String userInfo = uri.getUserInfo();
                if (userInfo != null) {
                    // First we should check if default authentication info (user:password) is provided
                    int pos = userInfo.indexOf(':');
                    if (pos < 0) {
                        config.username = userInfo;
                    } else {
                        config.username = userInfo.substring(0, pos);
                        config.password = userInfo.substring(pos + 1);
                    }
                }
                xmlConfiguration = null;
            } else {
                throw new IllegalArgumentException("Unknown scheme given with specified URI: " + uri.getScheme());
            }
        } else {
            xmlConfiguration = null;
        }

        /* If XML configuration exists and was successfully parsed,
         * and if connection map got from XML configuration is not empty:
         * try to resolve InetSocketAddress of any available connection.
         * Keep configuration of that connection which resolve operation was succeeded
         */
        if (xmlConfiguration != null) {
            InetAddress resolvedInetAddress = null;
            // Get map of connections with unresolved InetSocketAddress
            Map<InetSocketAddress, TarantoolClientConfig> connections = xmlConfiguration.getAvailableConnections();
            for (Map.Entry<InetSocketAddress, TarantoolClientConfig> entry : connections.entrySet()) {
                InetSocketAddress unresolvedSocketAddress = entry.getKey();
                // Try to resolve, if fails - iterate next entry
                try {
                    resolvedInetAddress = InetAddress.getByName(unresolvedSocketAddress.getHostName());
                } catch (UnknownHostException e) {
                    continue;
                }
                // Get TarantoolClientConfig
                config = entry.getValue();
                // Store host and port if resolve succeeded
                host = unresolvedSocketAddress.getHostName();
                port = unresolvedSocketAddress.getPort();
                // Exit from this loop
                break;
            }

            if (resolvedInetAddress == null) {
                throw new TarantoolCacheException("Cannot resolve any inet address obtained from XML");
            }
        }
        // Create and resolve InetSocketAddress here
        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);

        SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
            @Override
            public SocketChannel get(int retryNumber, Throwable lastError) {
                if (lastError != null) {
                    lastError.printStackTrace(System.out);
                }
                try {
                    return SocketChannel.open(socketAddress);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        };

        tarantoolClient = new TarantoolClientImpl(socketChannelProvider, config) {{
            msgPackLite = JavaMsgPackLite.INSTANCE;
        }};
        tarantoolClient.syncOps().ping();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        tarantoolClient.close();
    }

    /**
     * Obtain the {@link XmlConfiguration}
     * @return XmlConfiguration or {@code null} if no XML resource provided
     */
    public XmlConfiguration getSessionConfiguration() {
        return xmlConfiguration;
    }

    /**
     * Gets synchronous operation performer interface,
     * which can be used to perform select, insert, ... and so on.
     * @return TarantoolClientOps<Integer ,   List < ?>, Object, List<?>>
     */
    public TarantoolClientOps<Integer, List<?>, Object, List<?>> syncOps() {
        return tarantoolClient.syncOps();
    }

    /**
     * Transaction control: begin transaction.
     */
    public void begin() {
        try {
            tarantoolClient.syncOps().call("box.begin", true);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Transaction control: commit transaction.
     */
    public void commit() {
        try {
            tarantoolClient.syncOps().call("box.commit");
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Transaction control: abort and roll back transaction.
     */
    public void rollback() {
        try {
            tarantoolClient.syncOps().call("box.rollback");
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }
}
