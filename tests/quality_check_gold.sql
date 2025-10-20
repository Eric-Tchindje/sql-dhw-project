--Foreign key intergrity (Dimension)

select *
from gold.fact_sales f
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
LEFT JOIN gold.dim_customers  c ON  f.customer_key = c.customer_key
where p.product_key is null or c.customer_key is null;
