import json
import os
import re
from typing import Optional, List
from datetime import datetime
from tortoise.transactions import in_transaction
from database.models import Company

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'data')
DEFAULT_JSON = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', '..', 'data', 'companies_list.json')


def _safe_float(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = val.strip()
        # remove commas and parentheses, keep minus and dot
        s = s.replace(',', '')
        m = re.search(r"-?[0-9]+(?:\.[0-9]+)?", s)
        if m:
            try:
                return float(m.group(0))
            except:
                return None
    return None


class CompanyRepository:
    @staticmethod
    async def create_company(
        company_data: dict,
        fund_mandate_id: Optional[int] = None
    ) -> Company:
        """Create a company record from a dict (raw JSON row)"""
        # Map common keys with fallbacks
        name = company_data.get('Company ') or company_data.get('Company') or company_data.get('company') or ''
        country = company_data.get('Country')
        sector = company_data.get('Sector')
        industry = company_data.get('Industry')
        revenue = _safe_float(company_data.get('Revenue'))
        dividend_yield = company_data.get('Dividend Yield')
        five_years_growth = _safe_float(company_data.get('5-Years Growth'))
        net_income = _safe_float(company_data.get('Net Income'))
        total_assets = _safe_float(company_data.get('Total Assets'))
        total_equity = _safe_float(company_data.get('Total Equity'))
        eps_forecast = company_data.get('EPS / Forecast') or company_data.get('EPS\u00a0/\u00a0Forecast') or company_data.get('EPS')
        ebitda = company_data.get('EBITDA')
        one_year_change = company_data.get('1-Year Change') or company_data.get('1-Year Change')
        pe_ratio = _safe_float(company_data.get('P/E Ratio') or company_data.get('P/E'))
        debt_equity = _safe_float(company_data.get('Debt / Equity'))
        price_book = _safe_float(company_data.get('Price/Book') or company_data.get('Price to Book'))
        return_on_equity = _safe_float(company_data.get('Return on Equity'))
        market_cap = company_data.get('Market Cap')
        gross_profit_margin = company_data.get('Gross Profit Margin')
        risks = company_data.get('Risks') if isinstance(company_data.get('Risks'), (dict, list)) else None

        attrs = {k: v for k, v in company_data.items()}

        company = await Company.create(
            fund_mandate_id=fund_mandate_id,
            company_name=name,
            country=country,
            sector=sector,
            industry=industry,
            revenue=revenue,
            dividend_yield=str(dividend_yield) if dividend_yield is not None else None,
            five_years_growth=five_years_growth,
            net_income=net_income,
            total_assets=total_assets,
            total_equity=total_equity,
            eps_forecast=eps_forecast,
            ebitda=ebitda,
            one_year_change=one_year_change,
            pe_ratio=pe_ratio,
            debt_equity=debt_equity,
            price_book=price_book,
            return_on_equity=return_on_equity,
            market_cap=market_cap,
            gross_profit_margin=gross_profit_margin,
            risks=risks,
            attributes=attrs
        )
        return company

    @staticmethod
    async def fetch_all_companies() -> List[Company]:
        return await Company.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def fetch_by_id(company_id: int) -> Optional[Company]:
        return await Company.get_or_none(id=company_id, deleted_at__isnull=True)

    @staticmethod
    async def soft_delete(company_id: int) -> bool:
        company = await CompanyRepository.fetch_by_id(company_id)
        if not company:
            return False
        company.deleted_at = datetime.utcnow()
        await company.save()
        return True

    @staticmethod
    async def hard_delete(company_id: int) -> bool:
        company = await Company.get_or_none(id=company_id)
        if not company:
            return False
        await company.delete()
        return True

    @staticmethod
    async def fetch_count() -> int:
        """Get total count of companies"""
        return await Company.filter(deleted_at__isnull=True).count()

    @staticmethod
    async def bulk_import_from_json(file_path: Optional[str] = None, fund_mandate_id: Optional[int] = None) -> int:
        """Read companies from JSON file and insert into DB. Returns number inserted."""
        path = file_path or os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'data', 'companies_list.json')
        path = os.path.abspath(path)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Companies JSON file not found: {path}")

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError('Expected JSON array of company objects')

        created = 0
        async with in_transaction():
            for item in data:
                try:
                    await CompanyRepository.create_company(item, fund_mandate_id=fund_mandate_id)
                    created += 1
                except Exception:
                    continue
        return created