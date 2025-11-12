package com.qmt.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * 数组求和 UDF - 兼容 Flink SQL ARRAY<ROW> 类型
 */
public class SumArrayQuantity extends ScalarFunction {

    /**
     * 接受 Object 类型，在运行时处理类型转换
     *
     * @param itemsObj Flink SQL 的 ARRAY<ROW> 对象
     * @return 数量总和
     */
    public Integer eval(Object itemsObj) {
        // 处理 null
        if (itemsObj == null) {
            return 0;
        }

        // Flink SQL 的 ARRAY 在运行时可能是 Object[] 或 Row[]
        Object[] items;

        if (itemsObj instanceof Object[]) {
            items = (Object[]) itemsObj;
        } else if (itemsObj instanceof Row[]) {
            items = (Row[]) itemsObj;
        } else {
            // 未知类型，返回 0
            return 0;
        }

        if (items.length == 0) {
            return 0;
        }

        int total = 0;

        // 遍历数组
        for (Object itemObj : items) {
            if (itemObj == null) {
                continue;
            }

            // 每个 item 应该是 Row 类型
            Row item;
            if (itemObj instanceof Row) {
                item = (Row) itemObj;
            } else {
                continue;
            }

            // 获取 Quantity 字段（索引 3）
            // ROW 结构: [0]=OrderItemId, [1]=SellerSKU, [2]=SupplySourceId, [3]=Quantity
            Object quantityObj = item.getField(3);

            if (quantityObj != null) {
                int quantity = 0;

                // 处理不同的数值类型
                if (quantityObj instanceof Integer) {
                    quantity = (Integer) quantityObj;
                } else if (quantityObj instanceof Long) {
                    quantity = ((Long) quantityObj).intValue();
                } else if (quantityObj instanceof Double) {
                    quantity = ((Double) quantityObj).intValue();
                } else if (quantityObj instanceof String) {
                    try {
                        quantity = Integer.parseInt((String) quantityObj);
                    } catch (NumberFormatException e) {
                        // 忽略无法解析的值
                        continue;
                    }
                }

                if (quantity > 0) {
                    total += quantity;
                }
            }
        }

        return total;
    }
}
