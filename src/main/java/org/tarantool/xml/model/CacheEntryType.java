
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;


/**
 * <p>Java class for cache-entry-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "cache-entry-type", propOrder = {
    "value"
})
public class CacheEntryType {

    @XmlValue
    protected String value;
    @XmlAttribute(name = "serializer")
    protected String serializer;
    @XmlAttribute(name = "copier")
    protected String copier;

    /**
     * Gets the value of the value property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value of the value property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Gets the value of the serializer property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSerializer() {
        return serializer;
    }

    /**
     * Sets the value of the serializer property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSerializer(String value) {
        this.serializer = value;
    }

    /**
     * Gets the value of the copier property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCopier() {
        return copier;
    }

    /**
     * Sets the value of the copier property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCopier(String value) {
        this.copier = value;
    }

}
