import asyncio
import csv
import time
import re
import os
import random
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from tqdm import tqdm

def clean_phone_number(phone):
    if phone == "N/A" or not phone: return "N/A"
    cleaned = re.sub(r'[^\d]', '', phone)
    return cleaned if cleaned else "N/A"

def clean_field(value):
    if not value or value == "N/A": return "N/A"
    cleaned = str(value).strip().replace('\n', ' ').replace('\r', ' ')
    cleaned = ' '.join(cleaned.split())
    return cleaned if cleaned else "N/A"

async def human_delay(min_s=1, max_s=2):
    await asyncio.sleep(random.uniform(min_s, max_s))

async def scrape_google_maps():
    search_term = "Electricians in Manchester"
    output_folder = 'scraped_data'
    os.makedirs(output_folder, exist_ok=True)
    
    # Generate filename
    current_date = datetime.now().strftime('%Y_%m_%d')
    filename = f"manchester_electricians_{current_date}.csv"
    filepath = os.path.join(output_folder, filename)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        stealth = Stealth()
        await stealth.apply_stealth_async(context)
        page = await context.new_page()

        # BLOCK IMAGES to save speed
        await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font"] else route.continue_())

        print(f"🚀 Starting Scrape for: {search_term}")
        await page.goto('https://www.google.com/maps', wait_until='networkidle')
        
        # Handle Cookies - try multiple selectors and wait longer
        print("🍪 Handling cookies and popups...")
        cookie_selectors = [
            'button[aria-label="Accept all"]',
            'button:has-text("Accept all")',
            'button:has-text("I agree")',
            'button[id*="accept"]',
            'button[class*="accept"]'
        ]
        for selector in cookie_selectors:
            try:
                await page.click(selector, timeout=5000)
                print(f"   ✅ Clicked cookie button: {selector}")
                await asyncio.sleep(1)
                break
            except:
                continue
        
        # Wait a bit for any popups to settle
        await asyncio.sleep(2)

        # Search
        print(f"🔍 Searching for: {search_term}")
        try:
            # Wait for search box to be ready
            await page.wait_for_selector('#searchboxinput', timeout=10000)
            await page.fill('#searchboxinput', search_term)
            await page.keyboard.press('Enter')
            print("   ✅ Search submitted, waiting for results...")
        except Exception as e:
            print(f"   ⚠️  Search box error: {e}")
            raise
        
        # Wait for search to complete - check URL change or wait for results
        try:
            # Wait for URL to change (indicating search started)
            await page.wait_for_function(
                '() => window.location.href.includes("/search/") || window.location.href.includes("@")',
                timeout=15000
            )
            print("   ✅ Search URL detected")
        except:
            print("   ⚠️  URL change not detected, continuing anyway...")
        
        # Wait for results - try multiple selectors with longer timeout
        print("   ⏳ Waiting for results sidebar...")
        feed_selectors = [
            'div[role="feed"]',
            'div[role="main"] div[role="feed"]',
            '[data-value="Directions"]',  # Alternative indicator that results loaded
            'div[role="article"]'  # Direct listing selector
        ]
        
        feed_found = False
        for selector in feed_selectors:
            try:
                await page.wait_for_selector(selector, timeout=15000)
                print(f"   ✅ Found results using selector: {selector}")
                feed_found = True
                await asyncio.sleep(2)  # Give it a moment to fully load
                break
            except:
                continue
        
        if not feed_found:
            print("   ⚠️  Feed selector not found, but continuing anyway...")
            print("   💡 Trying to proceed with available content...")
            await asyncio.sleep(3)  # Wait a bit more

        # COMBINED SCROLLING & EXTRACTION - Continue until 60 unique leads
        print("📜 Starting scroll and extraction loop to find 60 unique leads...")
        print("   Target: div[role='feed'] (sidebar)\n")
        
        # Try to find sidebar with multiple selectors
        sidebar = None
        sidebar_selectors = [
            'div[role="feed"]',
            'div[role="main"] div[role="feed"]',
            'div[jsaction*="pane.result-item"]',  # Alternative selector
        ]
        
        for selector in sidebar_selectors:
            try:
                locator = page.locator(selector).first
                if await locator.count() > 0:
                    sidebar = locator
                    print(f"   ✅ Found sidebar using: {selector}\n")
                    break
            except:
                continue
        
        # Fallback: use page itself for scrolling if sidebar not found
        if sidebar is None:
            print("   ⚠️  Sidebar not found, will use page scrolling as fallback\n")
            sidebar = page  # Use page as fallback for scrolling
        target_unique_leads = 60
        max_scrolls = 30  # Increased to allow more scrolling
        scroll_count = 0
        previous_count = 0
        processed_indices = set()  # Track which listings we've already processed
        
        # Wait for sidebar to be visible (with more flexibility)
        if sidebar != page:
            try:
                await sidebar.wait_for(state='visible', timeout=5000)
                print("✅ Sidebar found and visible!\n")
            except:
                print("⚠️  Sidebar not immediately visible, checking for listings...\n")
                # Try to find listings directly as fallback
                listings_check = await page.locator('div[role="article"]').count()
                if listings_check > 0:
                    print(f"   ✅ Found {listings_check} listings directly, continuing...\n")
                else:
                    print("   ⚠️  No listings found yet, will retry during extraction...\n")
        else:
            # If using page as fallback, just check for listings
            print("⚠️  Using page scrolling fallback, checking for listings...\n")
            listings_check = await page.locator('div[role="article"]').count()
            if listings_check > 0:
                print(f"   ✅ Found {listings_check} listings, continuing...\n")
            else:
                print("   ⚠️  No listings found yet, will retry during extraction...\n")
        
        # EXTRACTION with Robust Selectors
        print("🔎 Extracting Data from listings...\n")
        leads = []
        seen_leads = set()  # Track unique business names for deduplication
        
        # Combined loop: scroll and extract until we have 60 unique leads
        while len(leads) < target_unique_leads and scroll_count < max_scrolls:
            # Get current listings
            listings = await page.locator('div[role="article"]').all()
            current_count = len(listings)
            
            # Extract from new listings we haven't processed yet
            new_leads_found = 0
            for index, listing in enumerate(listings):
                # Stop if we have enough unique leads
                if len(leads) >= target_unique_leads:
                    break
                
                # Skip if we've already processed this listing
                if index in processed_indices:
                    continue
                
                try:
                    # 1. Business Name - Robust Selectors (try both h3 and div.fontHeadlineSmall)
                    name = "N/A"
                    try:
                        # Try h3 first
                        h3_elem = listing.locator('h3').first
                        if await h3_elem.count() > 0:
                            name = await h3_elem.inner_text(timeout=2000)
                    except:
                        pass
                    
                    # If h3 didn't work, try div.fontHeadlineSmall
                    if name == "N/A" or not name.strip():
                        try:
                            name_elem = listing.locator('div.fontHeadlineSmall').first
                            if await name_elem.count() > 0:
                                name = await name_elem.inner_text(timeout=2000)
                        except:
                            pass
                    
                    # Final fallback: get any text from the listing
                    if name == "N/A" or not name.strip():
                        try:
                            all_text = await listing.inner_text()
                            lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                            if lines:
                                name = lines[0]  # First non-empty line
                        except:
                            pass
                    
                    # Skip if we still don't have a name
                    if name == "N/A" or not name.strip():
                        processed_indices.add(index)  # Mark as processed even if no name
                        continue
                    
                    # Normalize name for duplicate checking
                    normalized_name = name.lower().strip()
                    
                    # Check if this business name has already been seen
                    if normalized_name in seen_leads:
                        processed_indices.add(index)  # Mark as processed
                        continue  # Skip duplicate
                    
                    # Add to seen_leads set
                    seen_leads.add(normalized_name)
                    processed_indices.add(index)  # Mark as processed
                    
                    # 2. Rating & Reviews (relative to THIS listing)
                    rating = "N/A"
                    reviews = "N/A"
                    try:
                        stars_elem = listing.locator('span[aria-label*="stars"], span[aria-label*="star"]').first
                        if await stars_elem.count() > 0:
                            label = await stars_elem.get_attribute('aria-label')
                            if label and 'star' in label.lower():
                                match = re.search(r'(\d+\.?\d*)', label)
                                if match:
                                    rating = match.group(1)
                        
                        # Find reviews (usually next to stars)
                        try:
                            reviews_elem = listing.locator('span.UY7F9').first
                            if await reviews_elem.count() > 0:
                                reviews_text = await reviews_elem.inner_text(timeout=1000)
                                reviews = re.sub(r'[^\d]', '', reviews_text)
                        except:
                            pass
                    except:
                        pass

                    # 3. Phone & Website
                    phone = "N/A"
                    website = "N/A"
                    try:
                        # Phone often appears in the text of the card
                        card_text = await listing.inner_text()
                        # Try UK phone number patterns
                        phone_match = re.search(r'(\+44\s?\d{4}|\d{5})\s?\d{6}', card_text)
                        if phone_match: 
                            phone = phone_match.group(0)
                    except:
                        pass
                    
                    # Website extraction with hover action and better selectors
                    try:
                        # Hover over the listing to trigger dynamic data loading
                        await listing.hover()
                        await asyncio.sleep(0.5)  # Small delay for data to load
                        
                        # Wait for website element (with error handling - won't crash if no website)
                        try:
                            website_elem = listing.locator('a[aria-label*="Website"]').first
                            await website_elem.wait_for(state='attached', timeout=2000)
                        except:
                            # Business might not have a website, that's okay
                            pass
                        
                        # Helper function to clean and validate URLs
                        def clean_and_validate_url(url):
                            """Clean Google redirects and validate URL is not a Google domain"""
                            if not url or not url.strip():
                                return "N/A"
                            
                            url = url.strip()
                            
                            # Clean Google redirect URLs
                            if 'google.com/url' in url or '/url?q=' in url:
                                try:
                                    # Try parsing as URL first
                                    parsed = urlparse(url)
                                    query_params = parse_qs(parsed.query)
                                    if 'q' in query_params:
                                        url = query_params['q'][0]
                                        url = unquote(url)  # Decode URL encoding
                                except:
                                    # If parsing fails, try regex extraction
                                    match = re.search(r'[?&]q=([^&]+)', url)
                                    if match:
                                        url = unquote(match.group(1))
                            
                            # REJECT any Google URLs (even after cleaning)
                            if 'google.com' in url.lower() or url.startswith('google.com/'):
                                return "N/A"
                            
                            # Validate it's a real URL
                            if not ('http://' in url or 'https://' in url):
                                return "N/A"
                            
                            # Reject invalid URL types
                            if url.startswith('javascript:') or url.startswith('mailto:') or url.startswith('tel:'):
                                return "N/A"
                            
                            return url
                        
                        # Try multiple selectors for website (prioritize specific ones)
                        website_selectors = [
                            'a[data-item-id*="authority"]',  # Most reliable for actual websites
                            'a[aria-label*="Website"]',      # Explicit website link
                            'a.l_52kX7B1Y__button',          # Specific Google Maps class
                        ]
                        
                        for selector in website_selectors:
                            try:
                                website_elem = listing.locator(selector).first
                                if await website_elem.count() > 0:
                                    raw_url = await website_elem.get_attribute('href')
                                    if raw_url:
                                        website = clean_and_validate_url(raw_url)
                                        if website != "N/A":
                                            break  # Found valid website
                            except:
                                continue
                        
                        # If still N/A, try looking for any external link (but exclude Google)
                        if website == "N/A":
                            try:
                                all_links = listing.locator('a[href^="http"], a[href^="https://"]').all()
                                for link in await all_links:
                                    try:
                                        raw_url = await link.get_attribute('href')
                                        if raw_url:
                                            cleaned = clean_and_validate_url(raw_url)
                                            if cleaned != "N/A":
                                                website = cleaned
                                                break
                                    except:
                                        continue
                            except:
                                pass
                        
                        # Final attempt: try one more time after a brief wait
                        if website == "N/A":
                            await asyncio.sleep(0.3)
                            try:
                                website_elem = listing.locator('a[data-item-id*="authority"], a[aria-label*="Website"]').first
                                if await website_elem.count() > 0:
                                    raw_url = await website_elem.get_attribute('href')
                                    if raw_url:
                                        website = clean_and_validate_url(raw_url)
                            except:
                                pass
                                
                    except Exception as e:
                        # If website extraction fails, just continue with N/A
                        website = "N/A"

                    leads.append({
                        'Business Name': clean_field(name),
                        'Phone Number': clean_phone_number(phone),
                        'Website': clean_field(website),
                        'Rating': clean_field(rating),
                        'Number of Reviews': clean_field(reviews)
                    })
                    new_leads_found += 1
                    print(f"   ✅ Found ({len(leads)}/{target_unique_leads}): {name}")
                    
                    # Stop if we have enough unique leads
                    if len(leads) >= target_unique_leads:
                        break

                except Exception as e:
                    # Skip if listing is empty/ad or has errors
                    processed_indices.add(index)  # Mark as processed even on error
                    continue
            
            # Print progress
            print(f"   📊 Unique Leads Found: {len(leads)}/{target_unique_leads}")
            
            # Check if we have enough unique leads
            if len(leads) >= target_unique_leads:
                print(f"✅ Reached target of {target_unique_leads} unique leads!\n")
                break
            
            # If we still need more leads, scroll to load more listings
            if len(leads) < target_unique_leads:
                # Check if count increased (if not, we might be stuck)
                if current_count == previous_count and scroll_count > 0:
                    print(f"   ⚠️  Count unchanged ({current_count}), continuing to scroll...")
                
                # Scroll down 1000 pixels on the sidebar
                try:
                    # Check if sidebar is actually a feed element or the page
                    if sidebar == page:
                        # Use mouse wheel for page scrolling
                        await page.mouse.wheel(0, 1000)
                    else:
                        # Try scrolling the sidebar element
                        await sidebar.evaluate('element => element.scrollTop += 1000')
                except Exception as e:
                    print(f"   ⚠️  Scroll error: {e}, trying alternative scroll method...")
                    # Fallback: try scrolling the page with mouse wheel
                    try:
                        await page.mouse.wheel(0, 1000)
                    except:
                        # Last resort: try keyboard scrolling
                        await page.keyboard.press('PageDown')
                
                # Wait 3 seconds for new data to load
                await asyncio.sleep(3)
                
                previous_count = current_count
                scroll_count += 1
        
        # Final status
        if scroll_count >= max_scrolls:
            print(f"⚠️  Reached maximum scroll limit ({max_scrolls}). Found {len(leads)} unique leads.\n")
        else:
            print(f"✅ Finished! Found {len(leads)} unique leads.\n")

        # FINAL SAVE
        if leads:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Business Name', 'Phone Number', 'Website', 'Rating', 'Number of Reviews'])
                writer.writeheader()
                writer.writerows(leads)
            print(f"\n{'='*60}")
            print(f"✨ SUCCESS! Saved {len(leads)} leads to:")
            print(f"   📁 {filepath}")
            print(f"{'='*60}\n")
        else:
            print("\n❌ No leads found. Try running the script again.")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_google_maps())