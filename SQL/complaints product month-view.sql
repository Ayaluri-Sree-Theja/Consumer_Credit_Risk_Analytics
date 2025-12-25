CREATE OR REPLACE VIEW vw_complaints_product_month AS
SELECT
    month_start,
    COALESCE(product, 'Unknown') AS product,
    complaints_count
FROM fact_complaints_by_product_month;
