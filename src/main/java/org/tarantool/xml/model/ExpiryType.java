
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for expiry-type complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="expiry-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;choice&gt;
 *         &lt;element name="class" type="fqcn-type"/&gt;
 *         &lt;element name="tti" type="time-type"/&gt;
 *         &lt;element name="ttl" type="time-type"/&gt;
 *         &lt;element name="none"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *       &lt;/choice&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "expiry-type", propOrder = {
    "clazz",
    "tti",
    "ttl",
    "none"
})
public class ExpiryType {

    @XmlElement(name = "class")
    protected String clazz;
    protected TimeType tti;
    protected TimeType ttl;
    protected ExpiryType.None none;

    /**
     * Gets the value of the clazz property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getClazz() {
        return clazz;
    }

    /**
     * Sets the value of the clazz property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setClazz(String value) {
        this.clazz = value;
    }

    /**
     * Gets the value of the tti property.
     * 
     * @return
     *     possible object is
     *     {@link TimeType }
     *     
     */
    public TimeType getTti() {
        return tti;
    }

    /**
     * Sets the value of the tti property.
     * 
     * @param value
     *     allowed object is
     *     {@link TimeType }
     *     
     */
    public void setTti(TimeType value) {
        this.tti = value;
    }

    /**
     * Gets the value of the ttl property.
     * 
     * @return
     *     possible object is
     *     {@link TimeType }
     *     
     */
    public TimeType getTtl() {
        return ttl;
    }

    /**
     * Sets the value of the ttl property.
     * 
     * @param value
     *     allowed object is
     *     {@link TimeType }
     *     
     */
    public void setTtl(TimeType value) {
        this.ttl = value;
    }

    /**
     * Gets the value of the none property.
     * 
     * @return
     *     possible object is
     *     {@link ExpiryType.None }
     *     
     */
    public ExpiryType.None getNone() {
        return none;
    }

    /**
     * Sets the value of the none property.
     * 
     * @param value
     *     allowed object is
     *     {@link ExpiryType.None }
     *     
     */
    public void setNone(ExpiryType.None value) {
        this.none = value;
    }


    /**
     * <p>Java class for anonymous complex type.
     * 
     * <p>The following schema fragment specifies the expected content contained within this class.
     * 
     * <pre>
     * &lt;complexType&gt;
     *   &lt;complexContent&gt;
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class None {


    }

}
