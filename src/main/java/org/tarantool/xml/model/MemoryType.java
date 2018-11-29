
package org.tarantool.xml.model;

import java.math.BigInteger;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;


/**
 * <p>Java class for memory-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "memory-type", propOrder = {
    "value"
})
public class MemoryType {

    @XmlValue
    @XmlSchemaType(name = "positiveInteger")
    protected BigInteger value;
    @XmlAttribute(name = "unit")
    protected MemoryUnit unit;

    /**
     * Gets the value of the value property.
     * 
     * @return
     *     possible object is
     *     {@link BigInteger }
     *     
     */
    public BigInteger getValue() {
        return value;
    }

    /**
     * Sets the value of the value property.
     * 
     * @param value
     *     allowed object is
     *     {@link BigInteger }
     *     
     */
    public void setValue(BigInteger value) {
        this.value = value;
    }

    /**
     * Gets the value of the unit property.
     * 
     * @return
     *     possible object is
     *     {@link MemoryUnit }
     *     
     */
    public MemoryUnit getUnit() {
        if (unit == null) {
            return MemoryUnit.B;
        } else {
            return unit;
        }
    }

    /**
     * Sets the value of the unit property.
     * 
     * @param value
     *     allowed object is
     *     {@link MemoryUnit }
     *     
     */
    public void setUnit(MemoryUnit value) {
        this.unit = value;
    }

}
