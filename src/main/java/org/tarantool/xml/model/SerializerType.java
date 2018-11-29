
package org.tarantool.xml.model;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;


/**
 * <p>Java class for serializer-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "serializer-type", propOrder = {
    "serializer"
})
public class SerializerType {

    protected List<SerializerType.Serializer> serializer;

    /**
     * Gets the value of the serializer property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the serializer property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSerializer().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link SerializerType.Serializer }
     * 
     * 
     */
    public List<SerializerType.Serializer> getSerializer() {
        if (serializer == null) {
            serializer = new ArrayList<SerializerType.Serializer>();
        }
        return this.serializer;
    }


    /**
     * <p>Java class for anonymous complex type.
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "value"
    })
    public static class Serializer {

        @XmlValue
        protected String value;
        @XmlAttribute(name = "type", required = true)
        protected String type;

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
         * Gets the value of the type property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getType() {
            return type;
        }

        /**
         * Sets the value of the type property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setType(String value) {
            this.type = value;
        }

    }

}
