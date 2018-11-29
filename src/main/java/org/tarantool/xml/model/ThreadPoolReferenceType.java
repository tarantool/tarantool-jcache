
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for thread-pool-reference-type complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="thread-pool-reference-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;attribute name="thread-pool" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "thread-pool-reference-type")
public class ThreadPoolReferenceType {

    @XmlAttribute(name = "thread-pool", required = true)
    protected String threadPool;

    /**
     * Gets the value of the threadPool property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getThreadPool() {
        return threadPool;
    }

    /**
     * Sets the value of the threadPool property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setThreadPool(String value) {
        this.threadPool = value;
    }

}
