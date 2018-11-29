/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tarantool.xml;

import org.tarantool.cache.ClassLoading;
import org.tarantool.cache.ExpiryPolicyBuilder;
import org.tarantool.TarantoolClientConfig;

import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.tarantool.xml.exceptions.XmlConfigurationException;
import org.tarantool.xml.model.ConnectionType;
import org.tarantool.xml.model.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Builds configuration stored in XML.
 * <p>
 * Instances of this class are not thread-safe.
 */
public class XmlConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(XmlConfiguration.class);

  private final URL xml;
  private final ClassLoader classLoader;
  private final Map<String, ClassLoader> cacheClassLoaders;

  private final Map<String, Configuration<?, ?>> cacheConfigurations = new HashMap<>();
  private final Map<String, ConfigurationParser.CacheTemplate> templates = new HashMap<>();
  private final Map<InetSocketAddress, TarantoolClientConfig> connectionProperties = new HashMap<>();

  private ConfigurationParser.CacheTemplate defaultCacheTemplate;

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url}
   * <p>
   * Parses the XML file at the {@code url} provided.
   *
   * @param url URL pointing to the XML file's location
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(URL url)
      throws XmlConfigurationException {
    this(url, ClassLoading.getDefaultClassLoader());
  }

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url} and using the provided
   * {@code classLoader} to load user types (e.g. key and value Class instances).
   * <p>
   * Parses the XML file at the {@code url} provided.
   *
   * @param url URL pointing to the XML file's location
   * @param classLoader ClassLoader to use to load user types.
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(URL url, final ClassLoader classLoader)
      throws XmlConfigurationException {
    this(url, classLoader, Collections.emptyMap());
  }

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url} and using the provided
   * {@code classLoader} to load user types (e.g. key and value Class instances). The {@code cacheClassLoaders} will
   * let you specify a different {@link java.lang.ClassLoader} to use for each {@link javax.cache.Cache} managed by
   * the {@link javax.cache.CacheManager} configured using this {@link org.tarantool.xml.XmlConfiguration}
   * <p>
   * Parses the XML file at the {@code url} provided.
   *
   * @param url URL pointing to the XML file's location
   * @param classLoader ClassLoader to use to load user types.
   * @param cacheClassLoaders the map with mappings between cache names and the corresponding class loaders
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(URL url, final ClassLoader classLoader, final Map<String, ClassLoader> cacheClassLoaders)
      throws XmlConfigurationException {
    if(url == null) {
      throw new NullPointerException("The url can not be null");
    }
    if(classLoader == null) {
      throw new NullPointerException("The classLoader can not be null");
    }
    if(cacheClassLoaders == null) {
      throw new NullPointerException("The cacheClassLoaders map can not be null");
    }
    this.xml = url;
    this.classLoader = classLoader;
    this.cacheClassLoaders = new HashMap<>(cacheClassLoaders);
    try {
      parseConfiguration();
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error parsing XML configuration at " + url, e);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void parseConfiguration()
      throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException, JAXBException, ParserConfigurationException {
    LOGGER.info("Loading Tarantool XML configuration from {}.", xml.getPath());
    ConfigurationParser configurationParser = new ConfigurationParser(xml.toExternalForm());

    for (ConfigurationParser.CacheDefinition cacheDefinition : configurationParser.getCacheElements()) {
      String alias = cacheDefinition.id();
      if(cacheConfigurations.containsKey(alias)) {
        throw new XmlConfigurationException("Two caches defined with the same alias: " + alias);
      }

      ClassLoader cacheClassLoader = cacheClassLoaders.get(alias);

      if (cacheClassLoader == null) {
        if (classLoader != null) {
          cacheClassLoader = classLoader;
        } else {
          cacheClassLoader = ClassLoading.getDefaultClassLoader();
        }
      }

      MutableConfiguration<Object,Object> configuration = new MutableConfiguration<Object, Object>();
      if (cacheDefinition.keyType() != null && cacheDefinition.valueType() != null) {
          Class keyType = getClassForName(cacheDefinition.keyType(), cacheClassLoader);
          Class valueType = getClassForName(cacheDefinition.valueType(), cacheClassLoader);
          configuration.setTypes(keyType, valueType);
      }

      configuration.setManagementEnabled(cacheDefinition.enableManagement());
      configuration.setStatisticsEnabled(cacheDefinition.enableStatistics());

      configuration.setExpiryPolicyFactory(getExpiry(cacheDefinition.expiry(), cacheClassLoader));
      handleListenersConfig(cacheDefinition.listenersConfig(), cacheClassLoader, configuration);
      cacheConfigurations.put(alias, configuration);
    }

    templates.putAll(configurationParser.getTemplates());
    defaultCacheTemplate = configurationParser.getDefaultTemplate();
    List<ConnectionType> connections = configurationParser.getConnections();
    if (connections != null) {
      for (ConnectionType connection : connections) {
        if (connection.host == null || connection.port == null) {
            throw new XmlConfigurationException("Invalid connection parameters");
        }
        InetSocketAddress socketAddress = InetSocketAddress.createUnresolved(connection.host, connection.port);
        TarantoolClientConfig tarantoolClientConfig = new TarantoolClientConfig();
        tarantoolClientConfig.username = connection.username;
        tarantoolClientConfig.password = connection.password;
        tarantoolClientConfig.defaultRequestSize = connection.defaultRequestSize;
        tarantoolClientConfig.predictedFutures = connection.predictedFutures;
        tarantoolClientConfig.writerThreadPriority = connection.writerThreadPriority;
        tarantoolClientConfig.readerThreadPriority = connection.readerThreadPriority;
        tarantoolClientConfig.sharedBufferSize = connection.sharedBufferSize;
        tarantoolClientConfig.directWriteFactor = connection.directWriteFactor;
        tarantoolClientConfig.initTimeoutMillis = connection.initTimeoutMillis;
        tarantoolClientConfig.writeTimeoutMillis = connection.writeTimeoutMillis;
        tarantoolClientConfig.useNewCall = true;
        connectionProperties.put(socketAddress, tarantoolClientConfig);
      }
    }
    if (connections.isEmpty()) {
        throw new XmlConfigurationException("No connection parameters found in XML");
    }
  }

  private Factory<ExpiryPolicy> getExpiry(ConfigurationParser.Expiry parsedExpiry, ClassLoader cacheClassLoader)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (parsedExpiry.isUserDef()) {
        return getInstanceOfName(parsedExpiry.type(), cacheClassLoader, ExpiryPolicyBuilder.class);
    }

    final ExpiryPolicyBuilder expiryFactory = ExpiryPolicyBuilder.expiry();
    if (parsedExpiry.isTTL()) {
        Duration ttl = new Duration(parsedExpiry.unit(), parsedExpiry.value());
        expiryFactory.setCreateDuration(ttl);
        expiryFactory.setAccessDuration(null);
        expiryFactory.setUpdateDuration(ttl);
    } else if (parsedExpiry.isTTI()) {
        Duration tti = new Duration(parsedExpiry.unit(), parsedExpiry.value());
        expiryFactory.setCreateDuration(tti);
        expiryFactory.setAccessDuration(tti);
        expiryFactory.setUpdateDuration(tti);
    }
    return expiryFactory;
  }

  private static <T> T getInstanceOfName(String name, ClassLoader classLoader, Class<T> type) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (name == null) {
      return null;
    }
    Class<?> klazz = getClassForName(name, classLoader);
    return klazz.asSubclass(type).newInstance();
  }

  private static Class<?> getClassForName(String name, ClassLoader classLoader) throws ClassNotFoundException {
    return Class.forName(name, true, classLoader);
  }

  /**
   * Exposes the URL where the XML file parsed or yet to be parsed was or will be sourced from.
   * @return The URL provided at object instantiation
   */
  public URL getURL() {
    return xml;
  }

  /**
   * Creates a new {@link Configuration} seeded with the cache-template configuration
   * by the given {@code name} in the XML configuration parsed using {@link #parseConfiguration()}.
   *
   * @param name the unique name identifying the cache-template element in the XML
   * @param keyType the type of keys for the {@link Configuration} to use, must
   *                match the {@code key-type} declared in the template if declared in XML
   * @param valueType the type of values for the {@link Configuration} to use, must
   *                  match the {@code value-type} declared in the template if declared in XML
   * @param <K> type of keys
   * @param <V> type of values
   *
   * @return the preconfigured {@link Configuration}
   *         or {@code null} if no cache-template for the provided {@code name}
   *
   * @throws IllegalArgumentException if {@code keyType} or {@code valueType} don't match the declared type(s) of the template
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  public <K, V> Configuration<K, V> getCacheConfigurationFromTemplate(final String name,
                                                                      final Class<K> keyType,
                                                                      final Class<V> valueType)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    final ConfigurationParser.CacheTemplate cacheTemplate = templates.get(name);
    if (cacheTemplate == null) {
      return null;
    }
    final ClassLoader defaultClassLoader = (classLoader != null) ? classLoader : ClassLoading.getDefaultClassLoader();

    Class<?> keyClass = getClassForName(cacheTemplate.keyType(), defaultClassLoader);
    Class<?> valueClass = getClassForName(cacheTemplate.valueType(), defaultClassLoader);
    if (keyType != null && cacheTemplate.keyType() != null && !keyClass.isAssignableFrom(keyType)) {
      throw new IllegalArgumentException("CacheTemplate '" + name + "' declares key type of " + cacheTemplate.keyType());
    }
    if (valueType != null && cacheTemplate.valueType() != null && !valueClass.isAssignableFrom(valueType)) {
      throw new IllegalArgumentException("CacheTemplate '" + name + "' declares value type of " + cacheTemplate.valueType());
    }

    MutableConfiguration<K,V> configuration = new MutableConfiguration<K, V>();
    configuration.setTypes(keyType, valueType);
    configuration.setManagementEnabled(cacheTemplate.enableManagement());
    configuration.setStatisticsEnabled(cacheTemplate.enableStatistics());
    configuration.setExpiryPolicyFactory(getExpiry(cacheTemplate.expiry(), defaultClassLoader));
    handleListenersConfig(cacheTemplate.listenersConfig(), defaultClassLoader, configuration);
    return configuration;
  }

  /**
   * Creates a new {@link Configuration} seeded with the default cache-template configuration.
   *
   * @return the preconfigured {@link Configuration}
   *         or {@code null} if no cache-template exists
   */
  public Configuration<?, ?> getDefaultCacheConfiguration() {
    if (defaultCacheTemplate == null) {
      return null;
    }
    final ClassLoader defaultClassLoader = (classLoader != null) ? classLoader : ClassLoading.getDefaultClassLoader();
    try {
        MutableConfiguration<?,?> configuration = new MutableConfiguration<Object, Object>();
        configuration.setManagementEnabled(defaultCacheTemplate.enableManagement());
        configuration.setStatisticsEnabled(defaultCacheTemplate.enableStatistics());
        configuration.setExpiryPolicyFactory(getExpiry(defaultCacheTemplate.expiry(), defaultClassLoader));
        handleListenersConfig(defaultCacheTemplate.listenersConfig(), defaultClassLoader, configuration);
        return configuration;
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        return null;
    }
  }

  private void handleListenersConfig(ConfigurationParser.ListenersConfig listenersConfig, ClassLoader defaultClassLoader, Configuration<?, ?> configuration) throws ClassNotFoundException {
    if (listenersConfig != null) {
      if (listenersConfig.listeners() != null) {
        for (ConfigurationParser.Listener listener : listenersConfig.listeners()) {
          final List<EventType> eventListToFireOn = listener.fireOn();
          for (EventType events : eventListToFireOn) {
            switch (events) {
              case CREATED:
                break;
              case EVICTED:
                break;
              case EXPIRED:
                break;
              case UPDATED:
                break;
              case REMOVED:
                break;
              default:
                throw new IllegalArgumentException("Invalid Event Type provided");
            }
          }
        }
      }
    }
  }

  /**
   * @return map of the preconfigured {@link Configuration}'s
   */
  public Configuration<?, ?> getCacheConfiguration(String aliasName) {
    return cacheConfigurations.get(aliasName);
  }

  /**
   * @return non-empty map of connections with unresolved InetSocketAddress
   */
  public Map<InetSocketAddress, TarantoolClientConfig> getAvailableConnections() {
    return connectionProperties;
  }
}
