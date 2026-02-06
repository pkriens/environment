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
    
    def __init__(self, verbose=False, category_url="https://www.aquo.nl/index.php/Categorie:Actueel"):
        self.errors = False
        self.verbose = verbose
        self.category_url = category_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def info(self, message):
        if self.verbose:
            print(message,file=sys.stderr)
    
    def error(self, message):
        self.errors = True
        print(message, file=sys.stderr)

    def extract_items_from_page(self, url):
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
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            
            # Check for "overige resultaten" link using xpath
            more_results_links = tree.xpath('//a[contains(text(), "overige resultaten")]')
            if more_results_links:
                href = more_results_links[0].get('href')
                if href:
                    more_url = self.resolve_relative_url(href)
                    
                    self.info(f"Found 'overige resultaten' link: {more_url}")
                    
                    # Get the "more results" page
                    more_response = self.session.get(more_url)
                    more_response.raise_for_status()
                    more_tree = html.fromstring(more_response.content)
                    
                    # Look for 500 items link using xpath
                    limit_500_links = more_tree.xpath('//a[contains(@href, "limit=500")]')
                    if limit_500_links:
                        base_href = limit_500_links[0].get('href')
                        if base_href:
                            # Get all data by fetching in batches of 500
                            self.info("Fetching data in batches of 500...")
                            all_rows = []
                            offset = 0
                            batch_size = 500
                            
                            while True:
                                # Construct URL for this batch
                                batch_href = base_href.replace('limit=500', f'limit={batch_size}')
                                # Update offset
                                import re
                                if 'offset=' in batch_href:
                                    batch_href = re.sub(r'offset=\d+', f'offset={offset}', batch_href)
                                else:
                                    batch_href += f'&offset={offset}'
                                
                                batch_url = self.resolve_relative_url(batch_href)
                                
                                self.info(f"Fetching batch {offset//batch_size + 1}: offset={offset}, limit={batch_size}")
                                
                                # Get this batch
                                batch_response = self.session.get(batch_url)
                                batch_response.raise_for_status()
                                batch_tree = html.fromstring(batch_response.content)
                                
                                # Extract rows from this batch
                                batch_rows = self._extract_table_from_page(batch_tree)
                                
                                if not batch_rows:
                                    # No more data
                                    self.info(f"No more data found at offset {offset}. Total rows collected: {len(all_rows)}")
                                    break
                                
                                all_rows.extend(batch_rows)
                                self.info(f"Collected {len(batch_rows)} rows in this batch, total so far: {len(all_rows)}")
                                
                                # Check if we got less than expected (indicating we're at the end)
                                if len(batch_rows) < batch_size:
                                    self.info(f"Last batch returned {len(batch_rows)} rows (less than {batch_size}), stopping pagination")
                                    break
                                
                                offset += batch_size
                            
                            # Return all collected rows
                            return all_rows
            
            # If no "overige resultaten" link, extract from current page
            return self._extract_table_from_page(tree)
            
        except Exception as e:
            self.error(f"Error extracting table from {url}: {e}")
            return []
    
    def _extract_table_from_page(self, tree):
        tables = tree.xpath('//table[@class="datatable"]')
        if not tables:
            # Try to find regular table with class="table" (static tables)
            tables = tree.xpath('//table[@class="table"]')
            if not tables:
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

    def crawl_category(self):
        self.info(f"Crawling: {self.category_url}")
        return self.extract_items_from_page(self.category_url)

    def crawl_id_page(self, id_url):
        """Extract table data from an ID page, handling pagination for large datasets."""
        self.info(f"Extracting table from: {id_url}")
        return self.extract_table_from_id_page(id_url)

    def get_page_title(self, url):
        """Return page title from h1.firstHeading or <title> tag."""
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

    def download_all_categories(self, output_dir="csvs", source_url=None):
        """Download all categories to specified directory."""
        # Create output directory relative to script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_output_path = os.path.join(script_dir, output_dir)
        os.makedirs(full_output_path, exist_ok=True)
        
        # Use provided URL or default URL to get all categories
        if source_url is None:
            source_url = self.category_url
        
        self.info(f"Fetching categories from: {source_url}")
        
        try:
            # Get all items using extract_items_from_page with source URL
            items = self.extract_items_from_page(source_url)
            
            categories = set()
            # Convert items to category names
            for item in items:
                category_name = item['id']
                categories.add(category_name)
            
            self.info(f"Found {len(categories)} categories to download")
            
            # Download each category
            for category in sorted(categories):
                try:
                    if category.startswith('Id-'):
                        # This is an ID page
                        url = self.resolve_url(category)
                        items = self.crawl_id_page(url)
                        
                        # Get the actual title from the page
                        page_title = self.get_page_title(url)
                        if page_title:
                            filename = f"{self.sanitize_filename(page_title)}.csv"
                        else:
                            filename = f"{category.replace('Id-', '')}.csv"
                    else:
                        self.error(f"Skipping non-ID category: {category}")
                        continue
                    
                    if items:
                        filepath = os.path.join(full_output_path, filename)
                        self.save_to_csv(items, filepath)
                        self.info(f"  Downloaded {len(items)} items from {category} -> {filename}")
                    else:
                        self.error(f"  No items found for {category}")
                        
                except Exception as e:
                    self.error(f"  Error downloading {category}: {e}")
                    # Continue to next category instead of stopping
                    
        except Exception as e:
            self.error(f"Error fetching categories: {e}")
            return

    def save_to_csv(self, items, filename):
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
        items = self.crawl_category()
        for item in items:
            if item.get('naam', '').lower() == name.lower():
                return item.get('id')
        return None
    
    def resolve_url(self, path_or_url):
        """Resolve a path or URL against the base Aquo domain."""
        if not path_or_url:
            return self.category_url
            
        # If it's already a full URL, use as-is
        if path_or_url.startswith('http'):
            return path_or_url
            
        # If it starts with /, it's a path from the root
        if path_or_url.startswith('/'):
            return f"https://www.aquo.nl{path_or_url}"
            
        # Otherwise, treat as a page path
        return f"https://www.aquo.nl/index.php/{path_or_url}"
    
    def resolve_relative_url(self, relative_path):
        """Resolve a relative URL against the base URL from category_url."""
        from urllib.parse import urlparse, urljoin
        
        # Parse the category_url to get the base URL
        parsed = urlparse(self.category_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # Handle different types of relative paths
        if relative_path.startswith('/'):
            # Absolute path from root
            return f"{base_url}{relative_path}"
        elif relative_path.startswith('http'):
            # Already absolute URL
            return relative_path
        else:
            # Relative path, add index.php prefix
            return f"{base_url}/index.php/{relative_path}"
    
    def resolve_parameter_to_url(self, param):
        """Convert any parameter (URL, ID, name, category) to appropriate URL"""
        # If it's already a URL, use as-is
        if param.startswith('http'):
            return param
        
        # If it's an ID, construct URL
        if param.startswith('Id-'):
            return self.resolve_url(param)
        
        # If it starts with Categorie:, it's a category
        if param.startswith('Categorie:'):
            return self.resolve_url(param)
            
        # If it contains a slash, assume it's a page path
        if '/' in param:
            return self.resolve_url(param)
        
        # Otherwise, try to find it as a domain name
        domain_id = self.find_id_by_name(param)
        if domain_id:
            return self.resolve_url(domain_id)
        return None
    
    def get_output_stream(self, output):
        """Return (stream, should_close) tuple for stdout or file output."""
        if output == '-':
            import sys
            return sys.stdout, False
        else:
            return open(output, 'w', newline='', encoding='utf-8'), True
    
    def print_table(self, items, output='-'):
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
    parser.add_argument('url', nargs='?', default='', 
                       help='Aquo URL, ID, category name, or domain name to process')
    parser.add_argument('-o', '--output', default='-', help='Output CSV file')
    parser.add_argument('-c', '--category', default='https://www.aquo.nl/index.php/Categorie:Actueel',
                       help='Base category URL (default: Categorie:Actueel)')
    parser.add_argument('--download', action='store_true', help='Download all categories to csvs directory')
    parser.add_argument('-q', '--query', metavar='NAME', help='Query domain name and return only the ID')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-d', '--dir', default="csvs", help='Directory to save downloaded CSV files')
    
    args = parser.parse_args()
    
    aquo = Aquo(verbose=args.verbose, category_url=args.category)    
    
    if args.query:
        domain_id = aquo.find_id_by_name(args.query)
        if domain_id:
            print(domain_id)  # Keep this as regular print since it's output
        else:
            aquo.error(f"No domain found with name '{args.query}'")
            sys.exit(1)
        return
    
    if args.download:
        aquo.download_all_categories(output_dir=args.dir)
        return


    if (args.url):        
        url = aquo.resolve_parameter_to_url(args.url)
        if ( url is None ):
            aquo.error(f"Could not resolve parameter to URL: {args.url}")
            sys.exit(1)
        else:
            items = aquo.crawl_id_page(url)
    else:
        items = aquo.crawl_category()
    
    if items:
        aquo.print_table(items, args.output)
    else:
        aquo.error("No items found")

if __name__ == "__main__":
    main()


