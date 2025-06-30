from playwright.async_api import async_playwright
from google_sheets import GoogleSheets
from model import ArticleModel
import asyncio

async def scrapper():
    sheets = GoogleSheets()
    existing_urls = sheets.get_existing_detail_urls() or set()  # Ensure fallback to empty set
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
        # category_urls = [await link.get_attribute('href') for link in category_links if await link.get_attribute('href')]
        # category_urls = list(dict.fromkeys(category_links))  # Remove duplicates

        if not category_links:
            print("⚠️ No categories found on homepage. Exiting.")
            await context.close()
            await browser.close()
            return

        print(f"📋 Found {len(category_links)} categories: {category_links}")

        for category_idx, category in enumerate(category_links):
            print(f"\n📌 Processing category {category_idx + 1}: {category}")
            next_url = category['url']
            page_number = 1

            while next_url:
                print(f"\n📄 Scraping page {page_number} of category: {category['name']}")
                await page.goto(next_url)
                await page.wait_for_load_state('networkidle')
                await page.evaluate("window.scrollBy(0, 1000)")
                await page.wait_for_timeout(2000)

                # Get article links
                await page.wait_for_selector("div.listghor_listing_wrapper")

                # Define the selector: include title links
                selector = (
                    "div.listghor_listing_wrapper div.listing_item_box h3 a, div.listghor_listing_wrapper div.listing_item_box a.listghor_link"
                )

                # Get article links
                article_links = await page.eval_on_selector_all(
                    selector,
                    "els => els.map(el => el.href).filter(href => href)"  # Filter out null/undefined hrefs
                )
                print(f"📋 Found {len(article_links)} articles on this page: {article_links}")

                # Remove duplicates and filter out already processed URLs
                article_links = list(dict.fromkeys(article_links))  # Remove duplicates while preserving order
                new_article_links = [url for url in article_links if url not in existing_urls]

                print(f"📋 Found {len(new_article_links)} articles on this page: {article_links}")
                if not article_links:
                    print("⚠️ No articles found on this page. Moving to next category.")
                    break

                for idx, url in enumerate(new_article_links):
                    if url not in existing_urls:
                        print(f"\n{idx + 1}. Visiting article: {url}")
                        article_page = await context.new_page()  # New page for each article
                        try:
                            await article_page.goto(url)
                            await article_page.wait_for_load_state('networkidle')

                            name = address = description = email = website = phone = category = facebook = "N/A"

                            if await article_page.locator("div.listing_details_content").is_visible():
                                container = article_page.locator("div.listing_details_content")

                                # Company Name
                                if await container.locator("h1.h2-class").count() > 0:
                                    name = (await container.locator("h1.h2-class").text_content()).strip()

                                # Category (inside the <span>)
                                if await container.locator("h2 span").count() > 0:
                                    category = await container.locator("h2 span").text_content()

                                if await container.locator("a[href^='//'], a[href^='http']").count() > 0:
                                    website_raw = await container.locator("a[href^='//'], a[href^='http']").first.get_attribute("href")
                                    if website_raw:
                                        website_raw = website_raw.strip()
                                        if website_raw.startswith("//"):
                                            website = "https:" + website_raw
                                        else:
                                            website = website_raw

                                # Phone
                                if await container.locator("a[href^='tel:']").count() > 0:
                                    phone = await container.locator("a[href^='tel:']").first.text_content()
                                    phone = phone.strip()

                                # Email
                                if await container.locator("a[href^='mailto:']").count() > 0:
                                    email_raw = await container.locator("a[href^='mailto:']").first.get_attribute("href")
                                    email = email_raw.replace("mailto:", "").strip()

                                # Category
                                if await article_page.locator("div.featured_list ul li").count() > 0:
                                    categories = await article_page.locator("div.featured_list ul li").all_text_contents()
                                    category = ", ".join([c.strip() for c in categories]) if categories else "N/A"

                            # Address
                            if await article_page.locator("div.contact_info h4.box_title:has-text('Location') + h5").count() > 0:
                                address = await article_page.locator("div.contact_info h4.box_title:has-text('Location') + h5").text_content()
                                address = address.strip() if address else "N/A"

                            # Company Description / Details
                            if await article_page.locator("div.text_editor").count() > 0:
                                description_raw = await article_page.locator("div.text_editor").text_content()
                                description = description_raw.strip().replace("\n", " ") if description_raw else "N/A"

                            # Facebook
                            if await article_page.locator("div.social_box a.facebook").count() > 0:
                                facebook = await article_page.locator("div.social_box a.facebook").get_attribute("href")
                                facebook = facebook.strip() if facebook else "N/A"


                            article = ArticleModel(
                                company_name=name,
                                company_details=description,
                                address=address,
                                detail_page_url=url,
                                source_url=next_url,
                                company_website=website,
                                company_email=email,
                                category=category,
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

                # Pagination within category
                try:
                    # Find all pagination list items
                    pagination_items = await page.locator("ul.className li").all()

                    next_li = None
                    for li in pagination_items:
                        text = (await li.text_content() or "").strip().lower()
                        if text == "next":
                            next_li = li
                            break

                    if next_li:
                        li_class = await next_li.get_attribute("class") or ""
                        if "disabled" in li_class.lower():
                            print("✅ Reached last page of category.")
                            next_url = None
                        else:
                            next_button = await next_li.locator("a")
                            print(f"➡️ Clicking next page...")
                            await next_button.click()
                            await page.wait_for_selector("div.listghor_listing_wrapper")  # Ensure content loads
                            page_number += 1
                    else:
                        print("✅ No Next button found, assuming last page.")
                        next_url = None
                except Exception as e:
                    print(f"❌ Failed to handle pagination: {e}")
                    next_url = None


        await context.close()
        await browser.close()

asyncio.run(scrapper())