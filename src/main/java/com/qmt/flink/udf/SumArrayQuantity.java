package com.qmt.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class SumArrayQuantity extends ScalarFunction {

    public Integer eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object arr) {
        if (arr == null) {
            return 0;
        }

        try {
            if (!arr.getClass().isArray()) {
                return 0;
            }

            int length = java.lang.reflect.Array.getLength(arr);
            if (length == 0) {
                return 0;
            }

            int total = 0;

            for (int i = 0; i < length; i++) {
                Object element = java.lang.reflect.Array.get(arr, i);

                if (element == null) {
                    continue;
                }

                Object qtyField = null;

                if (element instanceof org.apache.flink.types.Row) {
                    org.apache.flink.types.Row row = (org.apache.flink.types.Row) element;
                    qtyField = row.getField(3);
                } else {
                    try {
                        java.lang.reflect.Method getField = element.getClass().getMethod("getField", int.class);
                        qtyField = getField.invoke(element, 3);
                    } catch (Exception e) {
                        try {
                            java.lang.reflect.Field[] fields = element.getClass().getDeclaredFields();
                            if (fields.length > 3) {
                                fields[3].setAccessible(true);
                                qtyField = fields[3].get(element);
                            }
                        } catch (Exception ex) {
                            continue;
                        }
                    }
                }

                if (qtyField instanceof Integer) {
                    total += (Integer) qtyField;
                } else if (qtyField instanceof Long) {
                    total += ((Long) qtyField).intValue();
                } else if (qtyField instanceof Number) {
                    total += ((Number) qtyField).intValue();
                }
            }

            return total;

        } catch (Exception e) {
            return 0;
        }
    }
}
