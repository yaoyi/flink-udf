package com.qmt.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * 高性能数组求和 UDF
 *
 * 功能: 计算订单项数组中所有 Quantity 的总和
 *
 * 输入: ARRAY<ROW<OrderItemId STRING, SellerSKU STRING, SupplySourceId STRING, Quantity INT>>
 * 输出: INT (总和)
 *
 * 性能优化:
 * 1. 使用原始类型 int 避免装箱
 * 2. 直接数组访问，避免迭代器开销
 * 3. 空值检查优化
 *
 * 使用示例:
 * SELECT sum_array_quantity(order_items) FROM orders;
 */
@FunctionHint(
    input = @DataTypeHint("ARRAY<ROW<OrderItemId STRING, SellerSKU STRING, SupplySourceId STRING, Quantity INT>>"),
    output = @DataTypeHint("INT")
)
public class SumArrayQuantity extends ScalarFunction {

    /**
     * 计算数组中所有 Quantity 的总和
     *
     * @param items 订单项数组，格式: ROW<OrderItemId, SellerSKU, SupplySourceId, Quantity>
     * @return 数量总和，如果数组为空或 null 返回 0
     */
    public int eval(
        @DataTypeHint("ARRAY<ROW<OrderItemId STRING, SellerSKU STRING, SupplySourceId STRING, Quantity INT>>")
        Row[] items
    ) {
        // 快速路径: 空检查
        if (items == null || items.length == 0) {
            return 0;
        }

        // 累加数量（使用原始类型避免装箱）
        int total = 0;

        // 直接数组访问，性能最优
        for (int i = 0; i < items.length; i++) {
            Row item = items[i];

            // 跳过 null 行
            if (item == null) {
                continue;
            }

            // Quantity 在第 4 个位置（索引 3）
            // ROW 结构: [0]=OrderItemId, [1]=SellerSKU, [2]=SupplySourceId, [3]=Quantity
            Integer quantity = (Integer) item.getField(3);

            // 只累加非 null 且大于 0 的值
            if (quantity != null && quantity > 0) {
                total += quantity;
            }
        }

        return total;
    }

    /**
     * 可选: 重载方法，支持过滤条件
     * 这个版本可以在后续扩展时使用
     */
    public int eval(Row[] items, String filterField, String filterValue) {
        if (items == null || items.length == 0) {
            return 0;
        }

        int total = 0;
        for (Row item : items) {
            if (item == null) continue;

            // 这里可以添加过滤逻辑
            Integer quantity = (Integer) item.getField(3);
            if (quantity != null && quantity > 0) {
                total += quantity;
            }
        }

        return total;
    }
}
