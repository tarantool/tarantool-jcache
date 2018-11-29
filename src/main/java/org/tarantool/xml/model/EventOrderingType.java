
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for event-ordering-type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="event-ordering-type"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *     &lt;enumeration value="UNORDERED"/&gt;
 *     &lt;enumeration value="ORDERED"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 * 
 */
@XmlType(name = "event-ordering-type")
@XmlEnum
public enum EventOrderingType {

    UNORDERED,
    ORDERED;

    public String value() {
        return name();
    }

    public static EventOrderingType fromValue(String v) {
        return valueOf(v);
    }

}
