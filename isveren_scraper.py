import requests
import pandas as pd
import json
import time
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IsverenScraper:
    def __init__(self):
        self.base_url = "https://isveren.az/cv/"
        self.session = requests.Session()
        # Add headers to mimic browser request
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'DNT': '1',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://isveren.az/cv',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        })

    def get_page_data(self, page=1):
        """Fetch data from a specific page"""
        try:
            params = {'page': page}
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()

            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON for page {page}: {e}")
            return None

    def scrape_all_cvs(self):
        """Scrape all CV data from all pages"""
        logger.info("Starting to scrape CV data from isveren.az")

        all_cvs = []
        page = 1

        while True:
            logger.info(f"Fetching page {page}...")
            page_data = self.get_page_data(page)

            if not page_data or 'cv' not in page_data:
                logger.error(f"No data found for page {page}")
                break

            cv_data = page_data['cv']
            cvs = cv_data.get('data', [])

            if not cvs:
                logger.info(f"No CVs found on page {page}. Ending scrape.")
                break

            all_cvs.extend(cvs)
            logger.info(f"Found {len(cvs)} CVs on page {page}. Total: {len(all_cvs)}")

            # Check if we've reached the last page
            if page >= cv_data.get('last_page', 1):
                logger.info(f"Reached last page ({cv_data.get('last_page')})")
                break

            page += 1
            time.sleep(1)  # Be respectful to the server

        logger.info(f"Scraping completed. Total CVs collected: {len(all_cvs)}")
        return all_cvs

    def process_cv_data(self, cvs):
        """Process and flatten CV data for CSV/Excel export"""
        logger.info("Processing CV data...")

        processed_cvs = []

        for cv in cvs:
            try:
                # Parse JSON fields
                skills = self.parse_json_field(cv.get('skills', '[]'))
                languages = self.parse_json_field(cv.get('language', '[]'))
                experience = self.parse_json_field(cv.get('experience', '[]'))
                education = self.parse_json_field(cv.get('education', '[]'))
                hobbies = self.parse_json_field(cv.get('hobby', '[]'))

                # Extract user and city information
                user = cv.get('user', {})
                city = cv.get('city', {})
                working_hour = cv.get('working_hour', {})

                # Create flattened record
                record = {
                    'id': cv.get('id'),
                    'title': cv.get('title'),
                    'slug': cv.get('slug'),
                    'name': user.get('name'),
                    'surname': user.get('surname'),
                    'birthday': cv.get('birthday'),
                    'gender': 'Male' if cv.get('gender_status') == 1 else 'Female' if cv.get('gender_status') == 2 else 'Unknown',
                    'marital_status': self.get_marital_status(cv.get('married_status')),
                    'has_children': 'Yes' if cv.get('is_child') == 1 else 'No' if cv.get('is_child') == 2 else 'Unknown',
                    'city': city.get('name', {}).get('az', '') if isinstance(city.get('name'), dict) else city.get('name', ''),
                    'permanent_address': cv.get('permanent_address'),
                    'actual_address': cv.get('actual_address'),
                    'phone': cv.get('phone'),
                    'email': cv.get('email'),
                    'working_hour': working_hour.get('name', {}).get('az', '') if isinstance(working_hour.get('name'), dict) else working_hour.get('name', ''),
                    'min_salary': cv.get('min_salary'),
                    'max_salary': cv.get('max_salary'),
                    'desired_address': cv.get('desired_address'),
                    'skills': self.format_list_field(skills),
                    'languages': self.format_languages(languages),
                    'experience': self.format_experience(experience),
                    'education': self.format_education(education),
                    'hobbies': self.format_list_field(hobbies),
                    'motivation_letter': cv.get('motivation_letter', ''),
                    'note': cv.get('note', ''),
                    'views': cv.get('reads', 0),
                    'created_at': cv.get('created_at'),
                    'updated_at': cv.get('updated_at'),
                    'resume_file': cv.get('resume', ''),
                    'profile_image': user.get('image', ''),
                    'user_position': user.get('position', ''),
                    'user_email': user.get('email', ''),
                    'user_phone': user.get('phone', ''),
                    'category_id': cv.get('category_id'),
                    'parent_category_id': cv.get('parent_category_id'),
                    'status': cv.get('status'),
                    'is_premium': cv.get('is_premium', 0),
                    'share_count': cv.get('share', 0)
                }

                processed_cvs.append(record)

            except Exception as e:
                logger.error(f"Error processing CV {cv.get('id', 'unknown')}: {e}")
                continue

        logger.info(f"Successfully processed {len(processed_cvs)} CVs")
        return processed_cvs

    def parse_json_field(self, field_value):
        """Safely parse JSON field"""
        if not field_value or field_value == '[]':
            return []
        try:
            return json.loads(field_value)
        except (json.JSONDecodeError, TypeError):
            return []

    def get_marital_status(self, status):
        """Convert marital status code to text"""
        status_map = {
            1: 'Single',
            2: 'Married - No children',
            3: 'Married - Has children'
        }
        return status_map.get(status, 'Unknown')

    def format_list_field(self, items):
        """Format list items as comma-separated string"""
        if not items:
            return ''
        if isinstance(items, list):
            return ', '.join(str(item) for item in items if item)
        return str(items)

    def format_languages(self, languages):
        """Format language information"""
        if not languages:
            return ''
        formatted = []
        for lang in languages:
            if isinstance(lang, dict):
                name = lang.get('name', '')
                level = lang.get('currentlyWorked', '')
                if name:
                    formatted.append(f"{name} ({level})" if level else name)
        return ', '.join(formatted)

    def format_experience(self, experiences):
        """Format work experience information"""
        if not experiences:
            return ''
        formatted = []
        for exp in experiences:
            if isinstance(exp, dict):
                company = exp.get('company', '')
                position = exp.get('position', '')
                start_date = exp.get('skill_start_date', '')
                end_date = exp.get('skill_end_date', '')
                currently_working = exp.get('currentlyWorked', '0')

                exp_str = f"{position} at {company}" if company and position else company or position
                if start_date:
                    if currently_working == '1' or not end_date:
                        exp_str += f" ({start_date} - Present)"
                    else:
                        exp_str += f" ({start_date} - {end_date})"

                if exp_str:
                    formatted.append(exp_str)
        return ' | '.join(formatted)

    def format_education(self, educations):
        """Format education information"""
        if not educations:
            return ''
        formatted = []
        for edu in educations:
            if isinstance(edu, dict):
                name = edu.get('name', '')
                specialization = edu.get('specialization', '')
                level = edu.get('level', '')
                start_date = edu.get('education_start_date', '')
                end_date = edu.get('education_end_date', '')
                currently_studying = edu.get('currentlyStudying', '0')

                edu_str = name
                if specialization:
                    edu_str += f" - {specialization}"
                if start_date:
                    if currently_studying == '1' or not end_date:
                        edu_str += f" ({start_date} - Present)"
                    else:
                        edu_str += f" ({start_date} - {end_date})"

                if edu_str:
                    formatted.append(edu_str)
        return ' | '.join(formatted)

    def save_to_csv(self, data, filename=None):
        """Save data to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"isveren_cvs_{timestamp}.csv"

        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Data saved to CSV: {filename}")
        return filename

    def save_to_xlsx(self, data, filename=None):
        """Save data to Excel file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"isveren_cvs_{timestamp}.xlsx"

        df = pd.DataFrame(data)

        # Create Excel writer with formatting options
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='CVs')

            # Auto-adjust column widths
            worksheet = writer.sheets['CVs']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width

        logger.info(f"Data saved to Excel: {filename}")
        return filename

def main():
    """Main function to run the scraper"""
    scraper = IsverenScraper()

    try:
        # Scrape all CV data
        cvs = scraper.scrape_all_cvs()

        if not cvs:
            logger.error("No CV data was collected. Exiting.")
            return

        # Process the data
        processed_data = scraper.process_cv_data(cvs)

        if not processed_data:
            logger.error("No data was processed successfully. Exiting.")
            return

        # Save to both CSV and Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = scraper.save_to_csv(processed_data, f"isveren_cvs_{timestamp}.csv")
        xlsx_file = scraper.save_to_xlsx(processed_data, f"isveren_cvs_{timestamp}.xlsx")

        logger.info(f"Scraping completed successfully!")
        logger.info(f"Total CVs scraped: {len(processed_data)}")
        logger.info(f"Files created: {csv_file}, {xlsx_file}")

    except Exception as e:
        logger.error(f"An error occurred during scraping: {e}")
        raise

if __name__ == "__main__":
    main()