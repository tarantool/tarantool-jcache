
package org.tarantool.xml.model;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.tarantool.xml.model package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _Config_QNAME = new QName(XMLConstants.W3C_XML_SCHEMA_NS_URI, "config");
    private final static QName _Resource_QNAME = new QName(XMLConstants.W3C_XML_SCHEMA_NS_URI, "resource");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.tarantool.xml.model
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link ExpiryType }
     * 
     */
    public ExpiryType createExpiryType() {
        return new ExpiryType();
    }

    /**
     * Create an instance of {@link CacheLoaderWriterType }
     * 
     */
    public CacheLoaderWriterType createCacheLoaderWriterType() {
        return new CacheLoaderWriterType();
    }

    /**
     * Create an instance of {@link CacheLoaderWriterType.WriteBehind }
     * 
     */
    public CacheLoaderWriterType.WriteBehind createCacheLoaderWriterTypeWriteBehind() {
        return new CacheLoaderWriterType.WriteBehind();
    }

    /**
     * Create an instance of {@link ListenersType }
     * 
     */
    public ListenersType createListenersType() {
        return new ListenersType();
    }

    /**
     * Create an instance of {@link SizeofType }
     * 
     */
    public SizeofType createSizeofType() {
        return new SizeofType();
    }

    /**
     * Create an instance of {@link ThreadPoolsType }
     * 
     */
    public ThreadPoolsType createThreadPoolsType() {
        return new ThreadPoolsType();
    }

    /**
     * Create an instance of {@link CopierType }
     * 
     */
    public CopierType createCopierType() {
        return new CopierType();
    }

    /**
     * Create an instance of {@link SerializerType }
     * 
     */
    public SerializerType createSerializerType() {
        return new SerializerType();
    }

    /**
     * Create an instance of {@link ConfigType }
     * 
     */
    public ConfigType createConfigType() {
        return new ConfigType();
    }

    /**
     * Create an instance of {@link ResourceType }
     * 
     */
    public ResourceType createResourceType() {
        return new ResourceType();
    }

    /**
     * Create an instance of {@link MemoryType }
     * 
     */
    public MemoryType createMemoryType() {
        return new MemoryType();
    }

    /**
     * Create an instance of {@link ServiceType }
     * 
     */
    public ServiceType createServiceType() {
        return new ServiceType();
    }

    /**
     * Create an instance of {@link ThreadPoolReferenceType }
     * 
     */
    public ThreadPoolReferenceType createThreadPoolReferenceType() {
        return new ThreadPoolReferenceType();
    }

    /**
     * Create an instance of {@link CacheType }
     * 
     */
    public CacheType createCacheType() {
        return new CacheType();
    }

    /**
     * Create an instance of {@link CacheTemplateType }
     * 
     */
    public CacheTemplateType createCacheTemplateType() {
        return new CacheTemplateType();
    }

    /**
     * Create an instance of {@link CacheEntryType }
     * 
     */
    public CacheEntryType createCacheEntryType() {
        return new CacheEntryType();
    }

    /**
     * Create an instance of {@link TimeType }
     * 
     */
    public TimeType createTimeType() {
        return new TimeType();
    }

    /**
     * Create an instance of {@link ResourcesType }
     * 
     */
    public ResourcesType createResourcesType() {
        return new ResourcesType();
    }

    /**
     * Create an instance of {@link ExpiryType.None }
     * 
     */
    public ExpiryType.None createExpiryTypeNone() {
        return new ExpiryType.None();
    }

    /**
     * Create an instance of {@link CacheLoaderWriterType.WriteBehind.Batching }
     * 
     */
    public CacheLoaderWriterType.WriteBehind.Batching createCacheLoaderWriterTypeWriteBehindBatching() {
        return new CacheLoaderWriterType.WriteBehind.Batching();
    }

    /**
     * Create an instance of {@link ListenersType.Listener }
     * 
     */
    public ListenersType.Listener createListenersTypeListener() {
        return new ListenersType.Listener();
    }

    /**
     * Create an instance of {@link SizeofType.MaxObjectGraphSize }
     * 
     */
    public SizeofType.MaxObjectGraphSize createSizeofTypeMaxObjectGraphSize() {
        return new SizeofType.MaxObjectGraphSize();
    }

    /**
     * Create an instance of {@link ThreadPoolsType.ThreadPool }
     * 
     */
    public ThreadPoolsType.ThreadPool createThreadPoolsTypeThreadPool() {
        return new ThreadPoolsType.ThreadPool();
    }

    /**
     * Create an instance of {@link CopierType.Copier }
     * 
     */
    public CopierType.Copier createCopierTypeCopier() {
        return new CopierType.Copier();
    }

    /**
     * Create an instance of {@link SerializerType.Serializer }
     * 
     */
    public SerializerType.Serializer createSerializerTypeSerializer() {
        return new SerializerType.Serializer();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ConfigType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = XMLConstants.W3C_XML_SCHEMA_NS_URI, name = "config")
    public JAXBElement<ConfigType> createConfig(ConfigType value) {
        return new JAXBElement<ConfigType>(_Config_QNAME, ConfigType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Object }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = XMLConstants.W3C_XML_SCHEMA_NS_URI, name = "resource")
    public JAXBElement<Object> createResource(Object value) {
        return new JAXBElement<Object>(_Resource_QNAME, Object.class, null, value);
    }

}
