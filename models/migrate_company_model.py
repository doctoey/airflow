# dataclass
from dataclasses import dataclass
from typing import Optional

@dataclass
class CompanySecurities:
    id: Optional[int]
    security_id: Optional[int]
    name_th: Optional[str]
    name_en: Optional[str]
    business_type: Optional[str]
    product_description: Optional[str]
    juristic_id: Optional[str]
    phone_number: Optional[str]
    website_url: Optional[str]
    address_number: Optional[str]
    address_road: Optional[str]
    address_province: Optional[str]
    address_district: Optional[str]
    address_subdistrict: Optional[str]
    address_zipcode: Optional[str]
    revenue_amount: Optional[float]
    revenue_year: Optional[int]