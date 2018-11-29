
package org.tarantool.xml.model;

import java.math.BigInteger;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for cache-loader-writer-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "cache-loader-writer-type", propOrder = {
    "clazz",
    "writeBehind"
})
public class CacheLoaderWriterType {

    @XmlElement(name = "class", required = true)
    protected String clazz;
    @XmlElement(name = "write-behind")
    protected CacheLoaderWriterType.WriteBehind writeBehind;

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
     * Gets the value of the writeBehind property.
     * 
     * @return
     *     possible object is
     *     {@link CacheLoaderWriterType.WriteBehind }
     *     
     */
    public CacheLoaderWriterType.WriteBehind getWriteBehind() {
        return writeBehind;
    }

    /**
     * Sets the value of the writeBehind property.
     * 
     * @param value
     *     allowed object is
     *     {@link CacheLoaderWriterType.WriteBehind }
     *     
     */
    public void setWriteBehind(CacheLoaderWriterType.WriteBehind value) {
        this.writeBehind = value;
    }


    /**
     * <p>Java class for anonymous complex type.
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "batching",
        "nonBatching"
    })
    public static class WriteBehind {

        protected CacheLoaderWriterType.WriteBehind.Batching batching;
        @XmlElement(name = "non-batching")
        protected Object nonBatching;
        @XmlAttribute(name = "concurrency")
        @XmlSchemaType(name = "positiveInteger")
        protected BigInteger concurrency;
        @XmlAttribute(name = "size")
        @XmlSchemaType(name = "positiveInteger")
        protected BigInteger size;
        @XmlAttribute(name = "thread-pool")
        protected String threadPool;

        /**
         * Gets the value of the batching property.
         * 
         * @return
         *     possible object is
         *     {@link CacheLoaderWriterType.WriteBehind.Batching }
         *     
         */
        public CacheLoaderWriterType.WriteBehind.Batching getBatching() {
            return batching;
        }

        /**
         * Sets the value of the batching property.
         * 
         * @param value
         *     allowed object is
         *     {@link CacheLoaderWriterType.WriteBehind.Batching }
         *     
         */
        public void setBatching(CacheLoaderWriterType.WriteBehind.Batching value) {
            this.batching = value;
        }

        /**
         * Gets the value of the nonBatching property.
         * 
         * @return
         *     possible object is
         *     {@link Object }
         *     
         */
        public Object getNonBatching() {
            return nonBatching;
        }

        /**
         * Sets the value of the nonBatching property.
         * 
         * @param value
         *     allowed object is
         *     {@link Object }
         *     
         */
        public void setNonBatching(Object value) {
            this.nonBatching = value;
        }

        /**
         * Gets the value of the concurrency property.
         * 
         * @return
         *     possible object is
         *     {@link BigInteger }
         *     
         */
        public BigInteger getConcurrency() {
            if (concurrency == null) {
                return new BigInteger("1");
            } else {
                return concurrency;
            }
        }

        /**
         * Sets the value of the concurrency property.
         * 
         * @param value
         *     allowed object is
         *     {@link BigInteger }
         *     
         */
        public void setConcurrency(BigInteger value) {
            this.concurrency = value;
        }

        /**
         * Gets the value of the size property.
         * 
         * @return
         *     possible object is
         *     {@link BigInteger }
         *     
         */
        public BigInteger getSize() {
            if (size == null) {
                return new BigInteger("2147483647");
            } else {
                return size;
            }
        }

        /**
         * Sets the value of the size property.
         * 
         * @param value
         *     allowed object is
         *     {@link BigInteger }
         *     
         */
        public void setSize(BigInteger value) {
            this.size = value;
        }

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


        /**
         * <p>Java class for anonymous complex type.
         */
        @XmlAccessorType(XmlAccessType.FIELD)
        @XmlType(name = "", propOrder = {

        })
        public static class Batching {

            @XmlElement(name = "max-write-delay", required = true)
            protected TimeType maxWriteDelay;
            @XmlAttribute(name = "batch-size", required = true)
            @XmlSchemaType(name = "positiveInteger")
            protected BigInteger batchSize;
            @XmlAttribute(name = "coalesce")
            protected Boolean coalesce;

            /**
             * Gets the value of the maxWriteDelay property.
             * 
             * @return
             *     possible object is
             *     {@link TimeType }
             *     
             */
            public TimeType getMaxWriteDelay() {
                return maxWriteDelay;
            }

            /**
             * Sets the value of the maxWriteDelay property.
             * 
             * @param value
             *     allowed object is
             *     {@link TimeType }
             *     
             */
            public void setMaxWriteDelay(TimeType value) {
                this.maxWriteDelay = value;
            }

            /**
             * Gets the value of the batchSize property.
             * 
             * @return
             *     possible object is
             *     {@link BigInteger }
             *     
             */
            public BigInteger getBatchSize() {
                return batchSize;
            }

            /**
             * Sets the value of the batchSize property.
             * 
             * @param value
             *     allowed object is
             *     {@link BigInteger }
             *     
             */
            public void setBatchSize(BigInteger value) {
                this.batchSize = value;
            }

            /**
             * Gets the value of the coalesce property.
             * 
             * @return
             *     possible object is
             *     {@link Boolean }
             *     
             */
            public boolean isCoalesce() {
                if (coalesce == null) {
                    return false;
                } else {
                    return coalesce;
                }
            }

            /**
             * Sets the value of the coalesce property.
             * 
             * @param value
             *     allowed object is
             *     {@link Boolean }
             *     
             */
            public void setCoalesce(Boolean value) {
                this.coalesce = value;
            }

        }

    }

}
