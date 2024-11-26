SELECT
    cs.id AS id,
    cs.security_id AS security_id,
    cs.name_th AS name_th,
    cs.name_en AS name_en,
    cs.business_type AS business_type,
    cs.product_description AS product_description,

    cs.juristic_id AS juristic_id,
    cs.phone_number AS phone_number,
    cs.website_url AS website_url,
    cs.address_number AS address_number,
    cs.address_road AS address_road,
    cs.address_province AS address_province,
    cs.address_district AS address_district,
    cs.address_subdistrict AS address_subdistrict,
    cs.address_zipcode AS address_zipcode,


    cr.revenue AS revenue_amount,
    cr.year AS revenue_year

FROM
    company_securities cs

JOIN
    company_revenue cr

ON
    cs.companyRevenueId = cr.id 