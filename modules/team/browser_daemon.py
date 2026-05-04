import asyncio
from playwright.async_api import async_playwright
import uvicorn
from fastapi import FastAPI

app = FastAPI(title="Supernova Browser Daemon")
browser_context = None

@app.on_event("startup")
async def startup():
    global browser_context
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    browser_context = await browser.new_context()
    print("🚀 Browser Daemon Operational")

@app.get("/goto")
async def goto(url: str):
    page = await browser_context.new_page()
    await page.goto(url)
    title = await page.title()
    return {"status": "success", "url": url, "title": title}

@app.get("/screenshot")
async def screenshot(path: str = "staging_snapshot.png"):
    pages = browser_context.pages
    if not pages: return {"status": "error", "message": "no pages open"}
    await pages[-1].screenshot(path=path)
    return {"status": "success", "path": path}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
