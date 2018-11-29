
package org.tarantool.xml.model;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;


/**
 * <p>Java class for copier-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "copier-type", propOrder = {
    "copier"
})
public class CopierType {

    protected List<CopierType.Copier> copier;

    /**
     * Gets the value of the copier property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the copier property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getCopier().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link CopierType.Copier }
     * 
     * 
     */
    public List<CopierType.Copier> getCopier() {
        if (copier == null) {
            copier = new ArrayList<CopierType.Copier>();
        }
        return this.copier;
    }


    /**
     * <p>Java class for anonymous complex type.
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "value"
    })
    public static class Copier {

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
