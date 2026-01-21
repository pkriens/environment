#!/usr/bin/env python3
"""
Aquo domain extractor for extracting domains from category pages.
"""

import requests
import csv
import sys
import os
from lxml import html
from urllib.parse import urlparse, parse_qs
import argparse

class Aquo:
    def __init__(self, verbose=False):
        self.base_url = "https://www.aquo.nl"
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def info(self, message):
        """Print info message if verbose mode is enabled"""
        if self.verbose:
            print(message,file=sys.stderr)
    
    def error(self, message):
        """Print error message to stderr"""
        print(message, file=sys.stderr)

    def extract_items_from_page(self, url):
        """Extract items from page."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            
            # Find all relevant links
            links = tree.xpath('//ul//a[@href and @title and text()]')
            
            items = []
            for link in links:
                href = link.get('href', '')
                title = link.get('title', '')
                text = link.text_content().strip()
                
                # Filter for Aquo ID links
                if 'Id-' in href and text and title:
                    # Extract ID from URL
                    parsed_url = urlparse(href)
                    if parsed_url.path:
                        # Get the page name which contains the ID
                        page_name = parsed_url.path.split('/')[-1]
                        if 'Id-' in page_name:
                            aquo_id = page_name
                        else:
                            # Try query params
                            query_params = parse_qs(parsed_url.query)
                            if 'title' in query_params:
                                aquo_id = query_params['title'][0]
                            else:
                                aquo_id = page_name
                    else:
                        aquo_id = href
                    
                    items.append({
                        'id': aquo_id,
                        'naam': text
                    })
            
            return items
            
        except Exception as e:
            self.error(f"Error extracting from {url}: {e}")
            return []

    def extract_table_from_id_page(self, url):
        """Extract table data from an Id page."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            
            # Find the table with class="datatable" (dynamic tables)
            tables = tree.xpath('//table[@class="datatable"]')
            if not tables:
                # Try to find regular table with class="table" (static tables)
                tables = tree.xpath('//table[@class="table"]')
                if not tables:
                    self.error("No datatable or regular table found on page")
                    return []
            
            table = tables[0]
            
            # Extract header row
            headers = []
            header_cells = table.xpath('.//thead//th | .//tbody//tr[1]//th | .//tr[1]//th')
            for cell in header_cells:
                header_text = cell.text_content().strip()
                if header_text:
                    headers.append(header_text)
            
            # If no headers found in thead/tbody, get them from first row
            if not headers:
                first_row_cells = table.xpath('.//tbody//tr[1]//td | .//tr[1]//td')
                for i, cell in enumerate(first_row_cells):
                    headers.append(f"Column_{i+1}")
            
            # Extract data rows - skip header rows and footer rows
            rows = []
            data_rows = table.xpath('.//tbody//tr[not(contains(@class, "smwfooter"))] | .//tr[position()>1 and not(contains(@class, "smwfooter"))]')
            
            for row in data_rows:
                cells = row.xpath('.//td')
                if cells:  # Skip empty rows
                    row_data = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            # Get text content, handling links
                            text_content = cell.text_content().strip()
                            row_data[headers[i]] = text_content
                    if row_data:  # Only add non-empty rows
                        rows.append(row_data)
            
            return rows
            
        except Exception as e:
            self.error(f"Error extracting table from {url}: {e}")
            return []

    def crawl_category(self, category_url):
        """Crawl a category page and extract all items."""
        self.info(f"Crawling: {category_url}")
        return self.extract_items_from_page(category_url)

    def crawl_id_page(self, id_url):
        """Crawl an Id page and extract table data."""
        self.info(f"Extracting table from: {id_url}")
        return self.extract_table_from_id_page(id_url)

    def get_page_title(self, url):
        """Extract the page title from an Aquo page."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            
            # Try to find the page title in the h1 element
            title_elements = tree.xpath('//h1[@class="firstHeading"]')
            if title_elements:
                return title_elements[0].text_content().strip()
            
            # Fallback: try to find it in the title tag
            title_elements = tree.xpath('//title')
            if title_elements:
                title = title_elements[0].text_content().strip()
                # Remove " - AQUO" suffix if present
                if title.endswith(' - AQUO'):
                    title = title[:-7]
                return title
                
            return None
            
        except Exception as e:
            self.error(f"Error getting page title from {url}: {e}")
            return None

    def sanitize_filename(self, name):
        """Clean a name to be suitable for use as filename."""
        if not name:
            return "unknown"
        
        # Remove Id- prefix if present
        if name.startswith('Id-'):
            # For Id pages, we'll get the actual title from the page
            return name
            
        # Remove Categorie: prefix
        if name.startswith('Categorie:'):
            name = name[10:]
            
        # Replace problematic characters
        name = name.replace(' ', '_')
        name = name.replace('/', '_')
        name = name.replace('\\', '_')
        name = name.replace(':', '_')
        name = name.replace('*', '_')
        name = name.replace('?', '_')
        name = name.replace('"', '_')
        name = name.replace('<', '_')
        name = name.replace('>', '_')
        name = name.replace('|', '_')
        
        return name

    def download_all_categories(self, output_dir="csvs"):
        """Download all categories to specified directory."""
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get all categories from the main categories page
        categories_url = "https://www.aquo.nl/index.php/Categorie:Domeintabellen"
        self.info(f"Fetching categories from: {categories_url}")
        
        try:
            response = self.session.get(categories_url)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            
            # Find all category links
            category_links = tree.xpath('//a[contains(@href, "Categorie:")]')
            
            categories = set()  # Use set to avoid duplicates
            for link in category_links:
                href = link.get('href', '')
                if 'Categorie:' in href and not href.endswith('Categorie:'):
                    # Skip URLs with query parameters that cause issues
                    if '&from=' in href or '&until=' in href:
                        continue
                    
                    # Extract category name from href
                    if href.startswith('http'):
                        # Full URL - extract the page name
                        if 'title=' in href:
                            # Query parameter format
                            continue  # Skip these for now as they cause issues
                        else:
                            # Direct path format
                            category_name = href.split('/')[-1]
                    elif href.startswith('/'):
                        category_name = href.split('/')[-1]
                    else:
                        category_name = href
                    
                    # Skip certain meta categories
                    if category_name not in ['Categorie:Domeintabellen', 'Categorie:Elementen']:
                        categories.add(category_name)
            
            # Also try to get categories from the Actueel page
            actueel_url = "https://www.aquo.nl/index.php/Categorie:Actueel"
            items = self.extract_items_from_page(actueel_url)
            
            # For each item that looks like a category/domain, add it
            for item in items:
                category_name = f"Id-{item['id'].replace('Id-', '')}"
                categories.add(category_name)
            
            self.info(f"Found {len(categories)} categories to download")
            
            # Download each category
            for category in sorted(categories):
                try:
                    if category.startswith('Id-'):
                        # This is an ID page
                        if category.startswith('http'):
                            url = category  # Already a full URL
                        else:
                            url = f"https://www.aquo.nl/index.php/{category}"
                        items = self.crawl_id_page(url)
                        
                        # Get the actual title from the page
                        page_title = self.get_page_title(url)
                        if page_title:
                            filename = f"{self.sanitize_filename(page_title)}.csv"
                        else:
                            filename = f"{category.replace('Id-', '')}.csv"
                    else:
                        # This is a category page
                        if category.startswith('http'):
                            url = category  # Already a full URL
                        else:
                            url = f"https://www.aquo.nl/index.php/{category}"
                        items = self.crawl_category(url)
                        filename = f"{self.sanitize_filename(category)}.csv"
                    
                    if items:
                        filepath = os.path.join(output_dir, filename)
                        self.save_to_csv(items, filepath)
                        self.info(f"  Downloaded {len(items)} items from {category} -> {filename}")
                    else:
                        self.error(f"  No items found for {category}")
                        
                except Exception as e:
                    self.error(f"  Error downloading {category}: {e}")
                    
        except Exception as e:
            self.error(f"Error fetching categories: {e}")
            return

    def save_to_csv(self, items, filename):
        """Save items to CSV file."""
        if not items:
            self.info("No items to save")
            return
            
        # Get fieldnames from first item
        fieldnames = list(items[0].keys()) if items else []
            
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(items)
        
        self.info(f"Saved {len(items)} items to {filename}")
    
    def find_id_by_name(self, name):
        """Find domain ID by name in Categorie:Actueel"""
        items = self.crawl_category('https://www.aquo.nl/index.php/Categorie:Actueel')
        for item in items:
            if item.get('naam', '').lower() == name.lower():
                return item.get('id')
        return None
    
    def resolve_parameter_to_url(self, param):
        """Convert any parameter (URL, ID, name, category) to appropriate URL"""
        # If it's already a URL, use as-is
        if param.startswith('http'):
            return param
        
        # If it's an ID, construct URL
        if param.startswith('Id-'):
            return f'https://www.aquo.nl/index.php/{param}'
        
        # If it starts with Categorie:, it's a category
        if param.startswith('Categorie:'):
            return f'https://www.aquo.nl/index.php/{param}'
        
        # If it contains a slash, assume it's a page path
        if '/' in param:
            return f'https://www.aquo.nl/index.php/{param}'
        
        # Otherwise, try to find it as a domain name
        domain_id = self.find_id_by_name(param)
        if domain_id:
            return f'https://www.aquo.nl/index.php/{domain_id}'
        
        # Fallback: treat as category name
        category_name = param if param.startswith('Categorie:') else f'Categorie:{param}'
        return f'https://www.aquo.nl/index.php/{category_name}'
    
    def get_output_stream(self, output):
        """Get output stream and whether it should be closed"""
        if output == '-':
            import sys
            return sys.stdout, False
        else:
            return open(output, 'w', newline='', encoding='utf-8'), True
    
    def print_table(self, items, output='-'):
        """Print items as CSV to console or file"""
        if not items:
            self.error("No items found")
            return
        
        # Get all fieldnames
        fieldnames = list(items[0].keys())
        
        # Get output stream
        stream, should_close = self.get_output_stream(output)
        
        try:
            # Write CSV
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(items)
            
            # Print confirmation for file output
            if should_close:
                self.info(f"Saved {len(items)} items to {output}")
        finally:
            if should_close:
                stream.close()

def main():
    parser = argparse.ArgumentParser(
        description='Aquo domain extractor - Extract data from Dutch water management standards',
        epilog='''Examples:
  %(prog)s                           # Show Categorie:Actueel as CSV
  %(prog)s Waterbeheerder            # Show domain data as CSV
  %(prog)s Id-xxx                    # Show specific ID data as CSV
  %(prog)s --download                # Download all to aquo/ directory
  %(prog)s -q Waterbeheerder         # Get ID for domain name
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('url', nargs='?', default='https://www.aquo.nl/index.php/Categorie:Actueel', 
                       help='Aquo URL, ID, category name, or domain name to process')
    parser.add_argument('-o', '--output', default='-', help='Output CSV file')
    parser.add_argument('--download', action='store_true', help='Download all categories to csvs directory')
    parser.add_argument('-q', '--query', metavar='NAME', help='Query domain name and return only the ID')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    crawler = Aquo(verbose=args.verbose)
    
    if args.query:
        domain_id = crawler.find_id_by_name(args.query)
        if domain_id:
            print(domain_id)  # Keep this as regular print since it's output
        else:
            crawler.error(f"No domain found with name '{args.query}'")
            sys.exit(1)
        return
    
    if args.download:
        crawler.download_all_categories()
        return
    
    url = crawler.resolve_parameter_to_url(args.url)
    
    # Detect extraction method based on URL
    if '/Id-' in url:
        # This is an Id page - extract table
        items = crawler.crawl_id_page(url)
    else:
        # This is a category page - extract links
        items = crawler.crawl_category(url)
    
    if items:
        crawler.print_table(items, args.output)
    else:
        crawler.error("No items found")

if __name__ == "__main__":
    main()


limit=500
offset=10
q=[[Breder::Id-d665b5f3-2cb2-4646-bf27-9eba5bbebe0c]]+%[[Eind+geldigheid::%E2%89%A521+januari+2026%5D%5D&p=mainlabel%3DPagina%2Fformat%3Dtable%2Flink%3Dall%2Fheaders%3Dshow%2Fsearchlabel%3D%E2%80%A6-20overige-20resultaten%2Fclass%3Dtable&po=%3FId%0A%3FTaxonouder%0A%3FTaxonniveau%0A%3FVerwijsnaam%0A%3FTWNstatus%0A%3FTWNmutatiedatum%0A%3FNaam+Nederlands%3DNaam_Nederlands%0A%3FNaam%0A%3FAuteur%0A%3FBegin+geldigheid%0A%3FGerelateerd%0A&sort=Begin+geldigheid&order=descending&eq=no#search

