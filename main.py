from fastapi import FastAPI
from run_transcript import run_transcript_processor

import asyncio


app = FastAPI()

@app.get("/runTranscript/{ticker}/{fiscal_year}/{fiscal_quarter}")
async def run_transcript(
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: str
):
    asyncio.run(run_transcript_processor(ticker, fiscal_year, fiscal_quarter))
    return {
        "Ticker": ticker,
        "FiscalYear": fiscal_year,
        "FiscalQuarter": fiscal_quarter
    }
