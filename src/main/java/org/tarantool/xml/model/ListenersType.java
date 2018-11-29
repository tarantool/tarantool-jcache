
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
 * <p>Java class for listeners-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "listeners-type", propOrder = {
    "listener"
})
public class ListenersType {

    protected List<ListenersType.Listener> listener;
    @XmlAttribute(name = "dispatcher-thread-pool")
    protected String dispatcherThreadPool;
    @XmlAttribute(name = "dispatcher-concurrency")
    @XmlSchemaType(name = "positiveInteger")
    protected BigInteger dispatcherConcurrency;

    /**
     * Gets the value of the listener property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the listener property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getListener().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ListenersType.Listener }
     * 
     * 
     */
    public List<ListenersType.Listener> getListener() {
        if (listener == null) {
            listener = new ArrayList<ListenersType.Listener>();
        }
        return this.listener;
    }

    /**
     * Gets the value of the dispatcherThreadPool property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDispatcherThreadPool() {
        return dispatcherThreadPool;
    }

    /**
     * Sets the value of the dispatcherThreadPool property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDispatcherThreadPool(String value) {
        this.dispatcherThreadPool = value;
    }

    /**
     * Gets the value of the dispatcherConcurrency property.
     * 
     * @return
     *     possible object is
     *     {@link BigInteger }
     *     
     */
    public BigInteger getDispatcherConcurrency() {
        if (dispatcherConcurrency == null) {
            return new BigInteger("8");
        } else {
            return dispatcherConcurrency;
        }
    }

    /**
     * Sets the value of the dispatcherConcurrency property.
     * 
     * @param value
     *     allowed object is
     *     {@link BigInteger }
     *     
     */
    public void setDispatcherConcurrency(BigInteger value) {
        this.dispatcherConcurrency = value;
    }


    /**
     * <p>Java class for anonymous complex type.
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "clazz",
        "eventFiringMode",
        "eventOrderingMode",
        "eventsToFireOn"
    })
    public static class Listener {

        @XmlElement(name = "class", required = true)
        protected String clazz;
        @XmlElement(name = "event-firing-mode", required = true)
        @XmlSchemaType(name = "string")
        protected EventFiringType eventFiringMode;
        @XmlElement(name = "event-ordering-mode", required = true)
        @XmlSchemaType(name = "string")
        protected EventOrderingType eventOrderingMode;
        @XmlElement(name = "events-to-fire-on", required = true)
        @XmlSchemaType(name = "string")
        protected List<EventType> eventsToFireOn;

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
         * Gets the value of the eventFiringMode property.
         * 
         * @return
         *     possible object is
         *     {@link EventFiringType }
         *     
         */
        public EventFiringType getEventFiringMode() {
            return eventFiringMode;
        }

        /**
         * Sets the value of the eventFiringMode property.
         * 
         * @param value
         *     allowed object is
         *     {@link EventFiringType }
         *     
         */
        public void setEventFiringMode(EventFiringType value) {
            this.eventFiringMode = value;
        }

        /**
         * Gets the value of the eventOrderingMode property.
         * 
         * @return
         *     possible object is
         *     {@link EventOrderingType }
         *     
         */
        public EventOrderingType getEventOrderingMode() {
            return eventOrderingMode;
        }

        /**
         * Sets the value of the eventOrderingMode property.
         * 
         * @param value
         *     allowed object is
         *     {@link EventOrderingType }
         *     
         */
        public void setEventOrderingMode(EventOrderingType value) {
            this.eventOrderingMode = value;
        }

        /**
         * Gets the value of the eventsToFireOn property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the eventsToFireOn property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getEventsToFireOn().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link EventType }
         * 
         * 
         */
        public List<EventType> getEventsToFireOn() {
            if (eventsToFireOn == null) {
                eventsToFireOn = new ArrayList<EventType>();
            }
            return this.eventsToFireOn;
        }

    }

}
