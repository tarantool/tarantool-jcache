
package org.tarantool.xml.model;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for config-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "config-type", propOrder = {
    "connections",
    "service",
    "defaultSerializers",
    "defaultCopiers",
    "threadPools",
    "eventDispatch",
    "writeBehind",
    "defaultTemplate",
    "cacheOrCacheTemplate"
})
@XmlRootElement(name = "config")
public class ConfigType {
    @XmlElementWrapper(name = "connections")
    @XmlElement(name="connection")
    protected List<ConnectionType> connections;
    protected List<ServiceType> service;
    @XmlElement(name = "default-serializers")
    protected SerializerType defaultSerializers;
    @XmlElement(name = "default-copiers")
    protected CopierType defaultCopiers;
    @XmlElement(name = "thread-pools")
    protected ThreadPoolsType threadPools;
    @XmlElement(name = "event-dispatch")
    protected ThreadPoolReferenceType eventDispatch;
    @XmlElement(name = "write-behind")
    protected ThreadPoolReferenceType writeBehind;
    @XmlElement(name = "default-template")
    @XmlIDREF
    @XmlSchemaType(name = "IDREF")
    protected Object defaultTemplate;
    @XmlElements({
        @XmlElement(name = "cache", type = CacheType.class),
        @XmlElement(name = "cache-template", type = CacheTemplateType.class)
    })
    protected List<BaseCacheType> cacheOrCacheTemplate;

    /**
    * Gets list of connections.
    * 
    * @return
    *     possible object is
    *     {@link List<ConnectionType> }
    *     
    */
   public List<ConnectionType> getConnections() {
       return connections;
   }

   /**
    * Sets list of connections.
    * 
    * @param value
    *     allowed object is
    *     {@link List<ConnectionType> }
    *     
    */
   public void setConnections(List<ConnectionType> value) {
       this.connections = value;
   }

    /**
     * Gets the value of the service property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the service property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getService().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ServiceType }
     * 
     * 
     */
    public List<ServiceType> getService() {
        if (service == null) {
            service = new ArrayList<ServiceType>();
        }
        return this.service;
    }

    /**
     * Gets the value of the defaultSerializers property.
     * 
     * @return
     *     possible object is
     *     {@link SerializerType }
     *     
     */
    public SerializerType getDefaultSerializers() {
        return defaultSerializers;
    }

    /**
     * Sets the value of the defaultSerializers property.
     * 
     * @param value
     *     allowed object is
     *     {@link SerializerType }
     *     
     */
    public void setDefaultSerializers(SerializerType value) {
        this.defaultSerializers = value;
    }

    /**
     * Gets the value of the defaultCopiers property.
     * 
     * @return
     *     possible object is
     *     {@link CopierType }
     *     
     */
    public CopierType getDefaultCopiers() {
        return defaultCopiers;
    }

    /**
     * Sets the value of the defaultCopiers property.
     * 
     * @param value
     *     allowed object is
     *     {@link CopierType }
     *     
     */
    public void setDefaultCopiers(CopierType value) {
        this.defaultCopiers = value;
    }

    /**
     * Gets the value of the threadPools property.
     * 
     * @return
     *     possible object is
     *     {@link ThreadPoolsType }
     *     
     */
    public ThreadPoolsType getThreadPools() {
        return threadPools;
    }

    /**
     * Sets the value of the threadPools property.
     * 
     * @param value
     *     allowed object is
     *     {@link ThreadPoolsType }
     *     
     */
    public void setThreadPools(ThreadPoolsType value) {
        this.threadPools = value;
    }

    /**
     * Gets the value of the eventDispatch property.
     * 
     * @return
     *     possible object is
     *     {@link ThreadPoolReferenceType }
     *     
     */
    public ThreadPoolReferenceType getEventDispatch() {
        return eventDispatch;
    }

    /**
     * Sets the value of the eventDispatch property.
     * 
     * @param value
     *     allowed object is
     *     {@link ThreadPoolReferenceType }
     *     
     */
    public void setEventDispatch(ThreadPoolReferenceType value) {
        this.eventDispatch = value;
    }

    /**
     * Gets the value of the writeBehind property.
     * 
     * @return
     *     possible object is
     *     {@link ThreadPoolReferenceType }
     *     
     */
    public ThreadPoolReferenceType getWriteBehind() {
        return writeBehind;
    }

    /**
     * Sets the value of the writeBehind property.
     * 
     * @param value
     *     allowed object is
     *     {@link ThreadPoolReferenceType }
     *     
     */
    public void setWriteBehind(ThreadPoolReferenceType value) {
        this.writeBehind = value;
    }

    /**
    * Gets the value of the defaultTemplate property.
    * 
    * @return
    *     possible object is
    *     {@link Object }
    *     
    */
   public Object getDefaultTemplate() {
       return defaultTemplate;
   }

   /**
    * Sets the value of the defaultTemplate property.
    * 
    * @param value
    *     allowed object is
    *     {@link Object }
    *     
    */
   public void setDefaultTemplate(Object value) {
       this.defaultTemplate = value;
   }

    /**
     * Gets the value of the cacheOrCacheTemplate property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the cacheOrCacheTemplate property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getCacheOrCacheTemplate().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link CacheType }
     * {@link CacheTemplateType }
     * 
     * 
     */
    public List<BaseCacheType> getCacheOrCacheTemplate() {
        if (cacheOrCacheTemplate == null) {
            cacheOrCacheTemplate = new ArrayList<BaseCacheType>();
        }
        return this.cacheOrCacheTemplate;
    }

}
