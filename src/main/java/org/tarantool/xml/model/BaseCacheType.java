
package org.tarantool.xml.model;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;
import org.w3c.dom.Element;


/**
 * <p>Java class for base-cache-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "base-cache-type", propOrder = {
    "keyType",
    "valueType",
    "expiry",
    "evictionAdvisor",
    "loaderWriter",
    "resilience",
    "listeners",
    "resources",
    "enableManagement",
    "enableStatistics",
    "serviceConfiguration"
})
@XmlSeeAlso({
    CacheType.class,
    CacheTemplateType.class
})
public abstract class BaseCacheType {

    @XmlElement(name = "key-type", defaultValue = "java.lang.Object")
    protected CacheEntryType keyType;
    @XmlElement(name = "value-type", defaultValue = "java.lang.Object")
    protected CacheEntryType valueType;
    protected ExpiryType expiry;
    @XmlElement(name = "eviction-advisor")
    protected String evictionAdvisor;
    @XmlElement(name = "loader-writer")
    protected CacheLoaderWriterType loaderWriter;
    protected String resilience;
    protected ListenersType listeners;
    protected ResourcesType resources;
    @XmlAttribute(name = "enable-management")
    protected Boolean enableManagement;
    @XmlAttribute(name = "enable-statistics")
    protected Boolean enableStatistics;
    @XmlAnyElement
    protected List<Element> serviceConfiguration;

    /**
     * Gets the value of the keyType property.
     * 
     * @return
     *     possible object is
     *     {@link CacheEntryType }
     *     
     */
    public CacheEntryType getKeyType() {
        return keyType;
    }

    /**
     * Sets the value of the keyType property.
     * 
     * @param value
     *     allowed object is
     *     {@link CacheEntryType }
     *     
     */
    public void setKeyType(CacheEntryType value) {
        this.keyType = value;
    }

    /**
     * Gets the value of the valueType property.
     * 
     * @return
     *     possible object is
     *     {@link CacheEntryType }
     *     
     */
    public CacheEntryType getValueType() {
        return valueType;
    }

    /**
     * Sets the value of the valueType property.
     * 
     * @param value
     *     allowed object is
     *     {@link CacheEntryType }
     *     
     */
    public void setValueType(CacheEntryType value) {
        this.valueType = value;
    }

    /**
     * Gets the value of the expiry property.
     * 
     * @return
     *     possible object is
     *     {@link ExpiryType }
     *     
     */
    public ExpiryType getExpiry() {
        return expiry;
    }

    /**
     * Sets the value of the expiry property.
     * 
     * @param value
     *     allowed object is
     *     {@link ExpiryType }
     *     
     */
    public void setExpiry(ExpiryType value) {
        this.expiry = value;
    }

    /**
     * Gets the value of the evictionAdvisor property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getEvictionAdvisor() {
        return evictionAdvisor;
    }

    /**
     * Sets the value of the evictionAdvisor property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setEvictionAdvisor(String value) {
        this.evictionAdvisor = value;
    }

    /**
     * Gets the value of the loaderWriter property.
     * 
     * @return
     *     possible object is
     *     {@link CacheLoaderWriterType }
     *     
     */
    public CacheLoaderWriterType getLoaderWriter() {
        return loaderWriter;
    }

    /**
     * Sets the value of the loaderWriter property.
     * 
     * @param value
     *     allowed object is
     *     {@link CacheLoaderWriterType }
     *     
     */
    public void setLoaderWriter(CacheLoaderWriterType value) {
        this.loaderWriter = value;
    }

    /**
     * Gets the value of the resilience property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getResilience() {
        return resilience;
    }

    /**
     * Sets the value of the resilience property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setResilience(String value) {
        this.resilience = value;
    }

    /**
     * Gets the value of the listeners property.
     * 
     * @return
     *     possible object is
     *     {@link ListenersType }
     *     
     */
    public ListenersType getListeners() {
        return listeners;
    }

    /**
     * Sets the value of the listeners property.
     * 
     * @param value
     *     allowed object is
     *     {@link ListenersType }
     *     
     */
    public void setListeners(ListenersType value) {
        this.listeners = value;
    }

    /**
     * Gets the value of the resources property.
     * 
     * @return
     *     possible object is
     *     {@link ResourcesType }
     *     
     */
    public ResourcesType getResources() {
        return resources;
    }

    /**
     * Sets the value of the resources property.
     * 
     * @param value
     *     allowed object is
     *     {@link ResourcesType }
     *     
     */
    public void setResources(ResourcesType value) {
        this.resources = value;
    }

    /**
     * Gets the state of the enableManagement property.
     * 
     * @return
     *     {@link Boolean }
     *     
     */
    public Boolean getEnableManagement() {
        return this.enableManagement;
    }

    /**
     * Sets the state of the enableManagement property.
     * 
     * @param value
     *     {@link Boolean }
     *     
     */
    public void setEnableManagement(Boolean value) {
        this.enableManagement = value;
    }

    /**
     * Gets the state of the enableStatistics property.
     * 
     * @return
     *     {@link Boolean }
     *     
     */
    public Boolean getEnableStatistics() {
        return this.enableStatistics;
    }

    /**
     * Sets the state of the enableStatistics property.
     * 
     * @param value
     *     {@link Boolean }
     *     
     */
    public void setEnableStatistics(Boolean value) {
        this.enableStatistics = value;
    }

    /**
     * Gets the value of the serviceConfiguration property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the serviceConfiguration property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getServiceConfiguration().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Element }
     * 
     * 
     */
    public List<Element> getServiceConfiguration() {
        if (serviceConfiguration == null) {
            serviceConfiguration = new ArrayList<Element>();
        }
        return this.serviceConfiguration;
    }

}
