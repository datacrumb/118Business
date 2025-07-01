from playwright.async_api import async_playwright
from google_sheets import GoogleSheets
from model import ArticleModel
import asyncio
import json

async def scrapper():
    sheets = GoogleSheets()
    existing_urls = sheets.get_existing_detail_urls() or set()  # Ensure fallback to empty set
    category_counts = {}
    category_urls_seen = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()  # New context for isolation
        page = await context.new_page()
        page.set_default_timeout(60000)

        # Start from homepage
        homepage_url = "https://118businessdirectory.co.uk/business-categories"
        print(f"\n🏠 Scraping categories from homepage: {homepage_url}")
        await page.goto(homepage_url)
        await page.wait_for_load_state('networkidle')
        await page.evaluate("window.scrollBy(0, 1200)")
        await page.wait_for_timeout(2000)

        # Extract category links
        category_links = await page.eval_on_selector_all(
            "div.tags a",
            "elements => elements.map(el => ({ name: el.textContent.trim(), url: el.href }))"
        )

        if not category_links:
            print("⚠️ No categories found on homepage. Exiting.")
            await context.close()
            await browser.close()
            return

        print(f"📋 Found {len(category_links)} categories: {category_links}")

        for category_idx, category in enumerate(category_links):
            category_name = category.get('name', 'N/A')
            category_url = category.get('url')

            if not category_url:
                print(f"⚠️ Skipping category {category_name} due to missing URL.")
                continue

            await page.goto(category_url)
            page_number = 1

            while True:  # This loop will handle pagination
                print(f"\n📄 Scraping page {page_number} of category: {category_name}")
                await page.wait_for_load_state('networkidle')
                await page.evaluate("window.scrollBy(0, 1000)")
                await page.wait_for_timeout(2000)

                # Get article links
                await page.wait_for_selector("div.listghor_listing_wrapper")

                # Define the selector: only title links to avoid duplicates
                selector = "div.listghor_listing_wrapper div.listing_item_box h3 a"

                # Get article links
                article_links = await page.eval_on_selector_all(
                    selector,
                    "els => els.map(el => el.href).filter(href => href)"
                )

                # Remove duplicates and filter out already processed URLs
                article_links = list(dict.fromkeys(article_links))
                print(f"📋 Found {len(article_links)} unique articles on this page: {article_links}")

                new_article_links = [url for url in article_links if url not in existing_urls]
                
                if category_name not in category_urls_seen:
                    category_urls_seen[category_name] = set()

                # Count only new URLs per category
                new_total = 0
                for url in article_links:
                    if url not in category_urls_seen[category_name]:
                        category_urls_seen[category_name].add(url)
                        new_total += 1

                category_counts[category_name] = category_counts.get(category_name, 0) + new_total

                if not new_article_links and not article_links:
                    print("⚠️ No articles found on this page. Moving to next category.")
                    break

                for idx, url in enumerate(new_article_links):
                    # This check is redundant due to the list comprehension above, but it's a good safeguard.
                    if url not in existing_urls:
                        print(f"\n{idx + 1}. Visiting article: {url}")
                        article_page = await context.new_page()
                        try:
                            await article_page.goto(url)
                            await article_page.wait_for_load_state('networkidle')

                            name = address = description = email = website = phone = category_str = facebook = "N/A"

                            if await article_page.locator("div.listing_details_content").is_visible():
                                container = article_page.locator("div.listing_details_content")

                                if await container.locator("h1.h2-class").count() > 0:
                                    name_content = await container.locator("h1.h2-class").text_content()
                                    if name_content:
                                        name = name_content.strip()

                                if await container.locator("h2 span").count() > 0:
                                    category_content = await container.locator("h2 span").text_content()
                                    if category_content:
                                        category_str = category_content.strip()

                                if await container.locator("a[href^='//'], a[href^='http']").count() > 0:
                                    website_raw = await container.locator("a[href^='//'], a[href^='http']").first.get_attribute("href")
                                    if website_raw:
                                        website_raw = website_raw.strip()
                                        if website_raw.startswith("//"):
                                            website = "https:" + website_raw
                                        else:
                                            website = website_raw

                                if await container.locator("a[href^='tel:']").count() > 0:
                                    phone_content = await container.locator("a[href^='tel:']").first.text_content()
                                    if phone_content:
                                        phone = phone_content.strip()

                                if await container.locator("a[href^='mailto:']").count() > 0:
                                    email_raw = await container.locator("a[href^='mailto:']").first.get_attribute("href")
                                    if email_raw:
                                        email = email_raw.replace("mailto:", "").strip()

                            if await article_page.locator("div.contact_info h4.box_title:has-text('Location') + h5").count() > 0:
                                address_content = await article_page.locator("div.contact_info h4.box_title:has-text('Location') + h5").text_content()
                                if address_content:
                                    address = address_content.strip()

                            if await article_page.locator("div.text_editor").count() > 0:
                                description_raw = await article_page.locator("div.text_editor").text_content()
                                if description_raw:
                                    description = description_raw.strip().replace("\n", " ")

                            if await article_page.locator("div.social_box a.facebook").count() > 0:
                                facebook_href = await article_page.locator("div.social_box a.facebook").get_attribute("href")
                                if facebook_href:
                                    facebook = facebook_href.strip()

                            article = ArticleModel(
                                company_name=name,
                                company_details=description,
                                address=address,
                                detail_page_url=url,
                                source_url=page.url,
                                company_website=website,
                                company_email=email,
                                category=category_name,
                                phone=phone,
                                facebook=facebook,
                            )

                            print(f"📋 Scraped: {name}")
                            sheets.save_to_google_sheets([article])
                            existing_urls.add(url)
                            print(f"✅ Saved: {article}")

                        except Exception as e:
                            print(f"❌ Failed to scrape {url}: {e}")
                        finally:
                            await article_page.close()
                        await page.wait_for_timeout(1000)
                    else:
                        print(f"⚠️ Skipping duplicate: {url}")

                # Pagination logic
                next_page_link = page.locator(".listghor_pagination li:not(.disabled) a:has-text('Next')")

                if await next_page_link.count() > 0:
                    print(f"➡️ Clicking 'Next' page button to go to page {page_number + 1}.")
                    await next_page_link.click()
                    page_number += 1
                else:
                    print("✅ End of pagination for this category.")
                    # 🔄 Save progress after each category
                    with open("category_article_counts.json", "w") as f:
                        json.dump(category_counts, f, indent=2)
                    break

        await context.close()
        await browser.close()

asyncio.run(scrapper())