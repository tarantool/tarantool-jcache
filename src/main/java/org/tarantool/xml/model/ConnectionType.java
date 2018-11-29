
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for connection-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "connection-type", propOrder = {
    "host",
    "port",
    "username",
    "password",
    "defaultRequestSize",
    "predictedFutures",
    "writerThreadPriority",
    "readerThreadPriority",
    "sharedBufferSize",
    "directWriteFactor",
    "initTimeoutMillis",
    "writeTimeoutMillis"
})
public class ConnectionType {

    /**
     * host and port for connection to
     */
    @XmlAttribute(required = true)
    public String host;
    @XmlAttribute(required = true)
    public Integer port;

    /**
     * username and password for authorization
     */
    @XmlAttribute
    public String username;
    @XmlAttribute
    public String password;

    /**
     * default ByteArrayOutputStream size  when make query serialization
     */
    @XmlAttribute(name = "default_request_size")
    public Integer defaultRequestSize = 4096;

    /**
     * initial size for map which holds futures of sent request
     */
    @XmlAttribute(name = "predicted_futures")
    public Integer predictedFutures = (int) ((1024 * 1024) / 0.75) + 1;

    @XmlAttribute(name = "writer_thread_priority")
    public Integer writerThreadPriority = Thread.NORM_PRIORITY;

    @XmlAttribute(name = "reader_thread_priority")
    public Integer readerThreadPriority = Thread.NORM_PRIORITY;

    /**
     * shared buffer is place where client collect requests when socket is busy on write
     */
    @XmlAttribute(name = "shared_buffer_size")
    public Integer sharedBufferSize = 8 * 1024 * 1024;

    /**
     * not put request into the shared buffer if request size is ge directWriteFactor * sharedBufferSize
     */
    @XmlAttribute(name = "direct_write_factor")
    public Double directWriteFactor = 0.5d;

    /**
     * Any blocking ops timeout
     */
    @XmlAttribute(name = "init_timeout_millis")
    public Long initTimeoutMillis = 60*1000L;

    @XmlAttribute(name = "write_timeout_millis")
    public Long writeTimeoutMillis = 60*1000L;
}
