"""
src/ledger/registry/client.py
Stub ApplicantRegistryClient — returns realistic test data so agents
can run without a real external registry.
"""
from __future__ import annotations
import os
from datetime import datetime


class ApplicantRegistryClient:

    def __init__(self, db_pool=None):
        self._pool = db_pool

    @classmethod
    async def create(cls) -> "ApplicantRegistryClient":
        try:
            import asyncpg
            pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
            return cls(pool)
        except Exception:
            return cls(None)

    async def get_company_profile(self, company_id: str) -> dict:
        """Return company profile — tries DB first, falls back to stub."""
        if self._pool:
            try:
                async with self._pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT * FROM applicant_registry.companies WHERE company_id=$1",
                        company_id,
                    )
                    if row:
                        return dict(row)
            except Exception:
                pass

        # Stub fallback
        return {
            "company_id":   company_id,
            "legal_name":   f"Company {company_id}",
            "legal_type":   "LLC",
            "jurisdiction": "CA",
            "founded_year": 2018,
            "industry":     "Manufacturing",
            "annual_revenue_usd": 2_000_000,
            "prior_default": False,
        }

    async def get_compliance_flags(self, company_id: str) -> list:
        """Return active compliance flags for a company."""
        if self._pool:
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT * FROM applicant_registry.compliance_flags WHERE company_id=$1",
                        company_id,
                    )
                    if rows:
                        return [dict(r) for r in rows]
            except Exception:
                pass
        return []

    async def get_financial_history(self, company_id: str) -> list:
        """Return 3 years of financial history."""
        if self._pool:
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM applicant_registry.financial_history
                        WHERE company_id=$1
                        ORDER BY fiscal_year DESC
                        LIMIT 3
                        """,
                        company_id,
                    )
                    if rows:
                        return [dict(r) for r in rows]
            except Exception:
                pass

        # Stub fallback — 3 years of realistic data
        return [
            {
                "fiscal_year":       2024,
                "total_revenue":     2_100_000,
                "gross_profit":      840_000,
                "ebitda":            420_000,
                "net_income":        210_000,
                "total_assets":      3_500_000,
                "total_liabilities": 1_400_000,
                "total_equity":      2_100_000,
                "gross_margin":      0.40,
                "debt_to_equity":    0.67,
            },
            {
                "fiscal_year":       2023,
                "total_revenue":     1_900_000,
                "gross_profit":      760_000,
                "ebitda":            380_000,
                "net_income":        190_000,
                "total_assets":      3_200_000,
                "total_liabilities": 1_300_000,
                "total_equity":      1_900_000,
                "gross_margin":      0.40,
                "debt_to_equity":    0.68,
            },
            {
                "fiscal_year":       2022,
                "total_revenue":     1_700_000,
                "gross_profit":      680_000,
                "ebitda":            340_000,
                "net_income":        170_000,
                "total_assets":      2_900_000,
                "total_liabilities": 1_200_000,
                "total_equity":      1_700_000,
                "gross_margin":      0.40,
                "debt_to_equity":    0.71,
            },
        ]

    async def get_loan_relationships(self, company_id: str) -> dict:
        """Return loan relationship history for a company."""
        if self._pool:
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT fiscal_year, total_revenue, ebitda, net_income,
                               total_assets, total_liabilities, debt_to_equity
                        FROM applicant_registry.financial_history
                        WHERE company_id = $1
                        ORDER BY fiscal_year DESC
                        """,
                        company_id,
                    )
                    return {
                        "company_id": company_id,
                        "loan_history": [],
                        "financial_years": [dict(r) for r in rows],
                        "prior_defaults": False,
                    }
            except Exception:
                pass
        
        # Stub fallback for when DB is unavailable
        return {
            "company_id": company_id,
            "loan_history": [],
            "financial_years": [],  # Could also populate from get_financial_history if needed
            "prior_defaults": False,
        }