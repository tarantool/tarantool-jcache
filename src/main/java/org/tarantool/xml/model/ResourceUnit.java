
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for resource-unit.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="resource-unit"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *     &lt;enumeration value="entries"/&gt;
 *     &lt;enumeration value="B"/&gt;
 *     &lt;enumeration value="kB"/&gt;
 *     &lt;enumeration value="MB"/&gt;
 *     &lt;enumeration value="GB"/&gt;
 *     &lt;enumeration value="TB"/&gt;
 *     &lt;enumeration value="PB"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 * 
 */
@XmlType(name = "resource-unit")
@XmlEnum
public enum ResourceUnit {

    @XmlEnumValue("entries")
    ENTRIES("entries"),
    B("B"),
    @XmlEnumValue("kB")
    K_B("kB"),
    MB("MB"),
    GB("GB"),
    TB("TB"),
    PB("PB");
    private final String value;

    ResourceUnit(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static ResourceUnit fromValue(String v) {
        for (ResourceUnit c: ResourceUnit.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
