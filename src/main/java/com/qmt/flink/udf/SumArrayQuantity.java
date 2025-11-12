package com.qmt.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * 数组求和 UDF - 使用 RAW 类型
 */
public class SumArrayQuantity extends ScalarFunction {

    /**
     * 使用 @DataTypeHint("RAW") 告诉 Flink 接受任意类型
     * Flink 会在运行时传入实际的数组对象
     */
    public Integer eval(@DataTypeHint("RAW") Object itemsObj) {
        if (itemsObj == null) {
            return 0;
        }

        // Flink 内部可能使用 GenericArrayData 或 Object[]
        Object[] items = null;

        // 尝试多种可能的类型
        if (itemsObj instanceof Object[]) {
            items = (Object[]) itemsObj;
        } else if (itemsObj.getClass().isArray()) {
            // 处理原始数组类型
            int length = java.lang.reflect.Array.getLength(itemsObj);
            items = new Object[length];
            for (int i = 0; i < length; i++) {
                items[i] = java.lang.reflect.Array.get(itemsObj, i);
            }
        } else {
            // 可能是 Flink 内部的 ArrayData 类型
            // 尝试通过反射调用 toArray() 方法
            try {
                java.lang.reflect.Method toArrayMethod = itemsObj.getClass().getMethod("toArray");
                Object result = toArrayMethod.invoke(itemsObj);
                if (result instanceof Object[]) {
                    items = (Object[]) result;
                }
            } catch (Exception e) {
                // 无法转换，返回 0
                return 0;
            }
        }

        if (items == null || items.length == 0) {
            return 0;
        }

        int total = 0;

        for (Object itemObj : items) {
            if (itemObj == null) {
                continue;
            }

            // 每个元素应该是 Row 类型
            Row item = null;
            if (itemObj instanceof Row) {
                item = (Row) itemObj;
            } else {
                // 可能是 Flink 内部的 RowData 类型，尝试转换
                try {
                    // 尝试通过 getField 方法访问
                    java.lang.reflect.Method getFieldMethod = itemObj.getClass().getMethod("getField", int.class);
                    Object quantityObj = getFieldMethod.invoke(itemObj, 3);
                    if (quantityObj instanceof Integer) {
                        total += (Integer) quantityObj;
                    }
                    continue;
                } catch (Exception e) {
                    // 转换失败，跳过
                    continue;
                }
            }

            // 标准的 Row 处理
            if (item != null) {
                Object quantityObj = item.getField(3);
                if (quantityObj instanceof Integer) {
                    total += (Integer) quantityObj;
                } else if (quantityObj instanceof Long) {
                    total += ((Long) quantityObj).intValue();
                }
            }
        }

        return total;
    }
}
