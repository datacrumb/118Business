from typing import Optional, List
from dataclasses import dataclass

@dataclass(frozen=True)
class ArticleModel:
    company_name: str
    company_details: str
    address: str
    detail_page_url: str
    source_url: str
    category: str
    company_website: str
    company_email: Optional[str]
    phone: Optional[str]
    facebook: Optional[str]