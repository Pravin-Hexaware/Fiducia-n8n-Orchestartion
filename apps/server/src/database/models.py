import enum
from enum import Enum
from tortoise import fields, models

class TimestampMixin(models.Model):
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)
    deleted_at = fields.DatetimeField(null=True)

    class Meta:
        abstract = True

class FundMandate(TimestampMixin):
    id = fields.IntField(pk=True)
    legal_name = fields.CharField(max_length=255)
    strategy_type = fields.CharField(max_length=255)
    vintage_year = fields.IntField()
    primary_analyst = fields.CharField(max_length=500)
    processing_date = fields.DatetimeField(null=True)
    target_count = fields.IntField(null=True)
    extracted_parameters = fields.ForeignKeyField('models.ExtractedParameters', related_name='fund_mandate', null=True)
    description = fields.TextField(null=True)

    class Meta:
        table = "fund_mandates"

class ExtractedParameters(TimestampMixin):
    id = fields.IntField(pk=True)
    raw_response = fields.JSONField(null=True)

    class Meta:
        table = "extracted_parameters"

class SourcingParameters(TimestampMixin):
    id = fields.IntField(pk=True)
    extracted_parameters = fields.ForeignKeyField('models.ExtractedParameters', related_name='sourcing_parameters', null=True)
    key = fields.CharField(max_length=256)
    value = fields.TextField()

    class Meta:
        table = "sourcing_parameters"

class ScreeningParameters(TimestampMixin):
    id = fields.IntField(pk=True)
    extracted_parameters = fields.ForeignKeyField('models.ExtractedParameters', related_name='screening_parameters', null=True)
    key = fields.CharField(max_length=256)
    value = fields.TextField()

    class Meta:
        table = "screening_parameters"

class RiskParameters(TimestampMixin):
    id = fields.IntField(pk=True)
    extracted_parameters = fields.ForeignKeyField('models.ExtractedParameters', related_name='risk_parameters', null=True)
    key = fields.CharField(max_length=256)
    value = fields.TextField()

    class Meta:
        table = "risk_parameters"

class Company(TimestampMixin):
    id = fields.IntField(pk=True)
    fund_mandate = fields.ForeignKeyField('models.FundMandate', related_name='companies', null=True)
    company_name = fields.CharField(max_length=500)
    country = fields.CharField(max_length=128, null=True)
    sector = fields.CharField(max_length=128, null=True)
    industry = fields.CharField(max_length=256, null=True)
    revenue = fields.FloatField(null=True)
    dividend_yield = fields.CharField(max_length=64, null=True)
    five_years_growth = fields.FloatField(null=True)
    net_income = fields.FloatField(null=True)
    total_assets = fields.FloatField(null=True)
    total_equity = fields.FloatField(null=True)
    eps_forecast = fields.CharField(max_length=128, null=True)
    ebitda = fields.CharField(max_length=128, null=True)
    one_year_change = fields.CharField(max_length=64, null=True)
    pe_ratio = fields.FloatField(null=True)
    debt_equity = fields.FloatField(null=True)
    price_book = fields.FloatField(null=True)
    return_on_equity = fields.FloatField(null=True)
    market_cap = fields.CharField(max_length=128, null=True)
    gross_profit_margin = fields.CharField(max_length=64, null=True)
    risks = fields.JSONField(null=True)
    attributes = fields.JSONField(null=True)

    class Meta:
        table = "companies"


class Sourcing(TimestampMixin):
    id = fields.IntField(pk=True)
    company_id = fields.IntField()
    company_data = fields.JSONField()
    fund_mandate = fields.ForeignKeyField('models.FundMandate', related_name='sourcings')
    selected_parameters = fields.JSONField()
    companies = fields.ManyToManyField('models.Company', related_name='in_sourcing')

    class Meta:
        table = "sourcing"


class Screening(TimestampMixin):
    id = fields.IntField(pk=True)
    fund_mandate = fields.ForeignKeyField('models.FundMandate', related_name='screenings', null=True)  # ← Change: Added null=True
    company = fields.ForeignKeyField('models.Company', related_name='screenings', null=True)
    selected_parameters = fields.JSONField()
    status = fields.CharField(max_length=50, null=True)
    reason = fields.TextField(null=True)
    raw_agent_output = fields.TextField(null=True)

    class Meta:
        table = "screening"

class RiskAnalysis(TimestampMixin):
    id = fields.IntField(pk=True)
    fund_mandate = fields.ForeignKeyField('models.FundMandate', related_name='risk_analyses', null=True)
    company = fields.ForeignKeyField('models.Company', related_name='risk_analyses', null=True)
    parameter_analysis = fields.JSONField(null=True)
    overall_result = fields.CharField(max_length=50, null=True)
    overall_assessment = fields.JSONField(null=True)

    class Meta:
        table = "risk_analysis"

class GeneratedDocument(TimestampMixin):
    id = fields.IntField(pk=True)
    fund_mandate = fields.ForeignKeyField('models.FundMandate', related_name='generated_documents')
    generated_content = fields.TextField()

    class Meta:
        table = "generated_documents"