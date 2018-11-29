
package org.tarantool.xml.model;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for thread-pools-type complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="thread-pools-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="thread-pool" maxOccurs="unbounded"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;attribute name="alias" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="default" type="{http://www.w3.org/2001/XMLSchema}boolean" default="false" /&gt;
 *                 &lt;attribute name="min-size" use="required" type="{http://www.w3.org/2001/XMLSchema}nonNegativeInteger" /&gt;
 *                 &lt;attribute name="max-size" use="required" type="{http://www.w3.org/2001/XMLSchema}positiveInteger" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "thread-pools-type", propOrder = {
    "threadPool"
})
public class ThreadPoolsType {

    @XmlElement(name = "thread-pool", required = true)
    protected List<ThreadPoolsType.ThreadPool> threadPool;

    /**
     * Gets the value of the threadPool property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the threadPool property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getThreadPool().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ThreadPoolsType.ThreadPool }
     * 
     * 
     */
    public List<ThreadPoolsType.ThreadPool> getThreadPool() {
        if (threadPool == null) {
            threadPool = new ArrayList<ThreadPoolsType.ThreadPool>();
        }
        return this.threadPool;
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
     *       &lt;attribute name="alias" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *       &lt;attribute name="default" type="{http://www.w3.org/2001/XMLSchema}boolean" default="false" /&gt;
     *       &lt;attribute name="min-size" use="required" type="{http://www.w3.org/2001/XMLSchema}nonNegativeInteger" /&gt;
     *       &lt;attribute name="max-size" use="required" type="{http://www.w3.org/2001/XMLSchema}positiveInteger" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class ThreadPool {

        @XmlAttribute(name = "alias", required = true)
        protected String alias;
        @XmlAttribute(name = "default")
        protected Boolean _default;
        @XmlAttribute(name = "min-size", required = true)
        @XmlSchemaType(name = "nonNegativeInteger")
        protected BigInteger minSize;
        @XmlAttribute(name = "max-size", required = true)
        @XmlSchemaType(name = "positiveInteger")
        protected BigInteger maxSize;

        /**
         * Gets the value of the alias property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getAlias() {
            return alias;
        }

        /**
         * Sets the value of the alias property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setAlias(String value) {
            this.alias = value;
        }

        /**
         * Gets the value of the default property.
         * 
         * @return
         *     possible object is
         *     {@link Boolean }
         *     
         */
        public boolean isDefault() {
            if (_default == null) {
                return false;
            } else {
                return _default;
            }
        }

        /**
         * Sets the value of the default property.
         * 
         * @param value
         *     allowed object is
         *     {@link Boolean }
         *     
         */
        public void setDefault(Boolean value) {
            this._default = value;
        }

        /**
         * Gets the value of the minSize property.
         * 
         * @return
         *     possible object is
         *     {@link BigInteger }
         *     
         */
        public BigInteger getMinSize() {
            return minSize;
        }

        /**
         * Sets the value of the minSize property.
         * 
         * @param value
         *     allowed object is
         *     {@link BigInteger }
         *     
         */
        public void setMinSize(BigInteger value) {
            this.minSize = value;
        }

        /**
         * Gets the value of the maxSize property.
         * 
         * @return
         *     possible object is
         *     {@link BigInteger }
         *     
         */
        public BigInteger getMaxSize() {
            return maxSize;
        }

        /**
         * Sets the value of the maxSize property.
         * 
         * @param value
         *     allowed object is
         *     {@link BigInteger }
         *     
         */
        public void setMaxSize(BigInteger value) {
            this.maxSize = value;
        }

    }

}
