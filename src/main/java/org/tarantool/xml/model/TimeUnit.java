
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for time-unit.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="time-unit"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *     &lt;enumeration value="nanos"/&gt;
 *     &lt;enumeration value="micros"/&gt;
 *     &lt;enumeration value="millis"/&gt;
 *     &lt;enumeration value="seconds"/&gt;
 *     &lt;enumeration value="minutes"/&gt;
 *     &lt;enumeration value="hours"/&gt;
 *     &lt;enumeration value="days"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 * 
 */
@XmlType(name = "time-unit")
@XmlEnum
public enum TimeUnit {

    @XmlEnumValue("nanos")
    NANOS("nanos"),
    @XmlEnumValue("micros")
    MICROS("micros"),
    @XmlEnumValue("millis")
    MILLIS("millis"),
    @XmlEnumValue("seconds")
    SECONDS("seconds"),
    @XmlEnumValue("minutes")
    MINUTES("minutes"),
    @XmlEnumValue("hours")
    HOURS("hours"),
    @XmlEnumValue("days")
    DAYS("days");
    private final String value;

    TimeUnit(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static TimeUnit fromValue(String v) {
        for (TimeUnit c: TimeUnit.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
