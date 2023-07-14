import os
import re
import docx2txt
import PyPDF2
import mammoth
import spacy
from spacy.matcher import Matcher
from spacy.matcher import PhraseMatcher
from datetime import date
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nlp = spacy.load('en_core_web_sm')

matcher = Matcher(nlp.vocab) 

file = os.getenv('SKILLS')
file = open(file, "r", encoding='utf-8')    
skill = [line.strip().lower() for line in file]
skillsmatcher = PhraseMatcher(nlp.vocab)
patterns = [nlp.make_doc(text) for text in skill if len(nlp.make_doc(text)) < 10]
skillsmatcher.add("Job title", None, *patterns)
header_file = os.getenv('HEADERS')
header_file = open(header_file, "r", encoding='utf-8')
headers = [line.strip().lower() for line in header_file]

work_and_employment = (
        'career profile',
        'employment history',
        'work history',
        'work experience',
        'experience',
        'professional experience',
        'professional background',
        'additional experience',
        'career related experience',
        'related experience',
        'programming experience',
        'freelance',
        'freelance experience',
        'army experience',
        'military experience',
        'military background',
    )

def parser(name):
    resume_file = os.getenv('PATHR')+f"{name}.pdf"
    _, file_extension = os.path.splitext(resume_file)
    if file_extension == '.pdf':
        with open(resume_file, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            text = ''
            for page in pdf_reader.pages:
                text += page.extract_text()
            full_string = re.sub(r'\n+', '\n', text)
            full_string = full_string.replace("\r", "\n")
            full_string = full_string.replace("\t", " ")

            # Remove awkward LaTeX bullet characters

            full_string = re.sub(r"\uf0b7", " ", full_string)
            full_string = re.sub(r"\(cid:\d{0,2}\)", " ", full_string)
            full_string = re.sub(r'• ', " ", full_string)

            # Split text blob into individual lines
            clean_text = full_string.splitlines(True)

            # Remove empty strings and whitespaces
            clean_text = [re.sub('\s+', ' ', line.strip()) for line in clean_text if line.strip()]

    elif file_extension == '.docx':
        with open(resume_file, 'rb') as docx_file:
            result = mammoth.convert_to_text(docx_file)
            text = result.value
            resume_lines = re.sub(r'\n+', '\n', text)
            resume_lines = clean_text.replace("\r", "\n").replace("\t", " ")  
            clean_text = clean_text.splitlines()  
            clean_text = [re.sub('\s+', ' ', line.strip()) for line in resume_lines if line.strip()]
    elif file_extension == '.doc':
        text = docx2txt.process(resume_file)
        esume_lines = re.sub(r'\n+', '\n', text)
        resume_lines = clean_text.replace("\r", "\n").replace("\t", " ")  
        clean_text = clean_text.splitlines()  
        clean_text = [re.sub('\s+', ' ', line.strip()) for line in resume_lines if line.strip()]
    else:
        return None
    
    # Extract the skills section
    stop_words = set(stopwords.words('english'))
    skills = []

    __nlp = nlp(text.lower())
    # Only run nlp.make_doc to speed things up

    matches = skillsmatcher(__nlp)
    for match_id, start, end in matches:
        span = __nlp[start:end]
        skills.append(span.text)
    skills = list(set(skills))
    
    matched = []
    for w in skills:
        if w not in stop_words:
            matched.append(w)
            
    skills = matched

    section_pattern = r"^[A-Z][^:]*:"
    for i, item in enumerate(clean_text):
    # Check if the current item is the start of the work experience section
        if item.lower() in work_and_employment:
            start = i + 1
        # Check if the current item is the end of the work experience section
        elif start is not None and any(item.startswith(header) for header in headers):
            end = i
            break

    if start is not None and end is None:
        end = len(clean_text)

# Extract the work experience section
    work_experience = " ".join(clean_text[start:end])

    def correct_year(result):
            if len(result) < 2:
                if int(result) > int(str(date.today().year)[-2:]):
                    result = str(int(str(date.today().year)[:-2]) - 1) + result
                else:
                    result = str(date.today().year)[:-2] + result
            return result

    experience = 0
    start_month = -1
    start_year = -1
    end_month = -1
    end_year = -1

    not_alpha_numeric = r'[^a-zA-Z\d]'
    number = r'(\d{2})'

    months_num = r'(01)|(02)|(03)|(04)|(05)|(06)|(07)|(08)|(09)|(10)|(11)|(12)'
    months_short = r'(jan)|(feb)|(mar)|(apr)|(may)|(jun)|(jul)|(aug)|(sep)|(oct)|(nov)|(dec)'
    months_long = r'(january)|(february)|(march)|(april)|(may)|(june)|(july)|(august)|(september)|(october)|(november)|(december)'
    month = r'(' + months_num + r'|' + months_short + r'|' + months_long + r')'
    regex_year = r'((20|19)(\d{2})|(\d{2}))'
    year = regex_year
    start_date = month + not_alpha_numeric + r"?" + year
    
    # end_date = r'((' + number + r'?' + not_alpha_numeric + r"?" + number + not_alpha_numeric + r"?" + year + r')|(present|current))'
    end_date = r'((' + number + r'?' + not_alpha_numeric + r"?" + month + not_alpha_numeric + r"?" + year + r')|(present|current|till date|today))'
    longer_year = r"((20|19)(\d{2}))"
    year_range = longer_year + r"(" + not_alpha_numeric + r"{1,4}|(\s*to\s*))" + r'(' + longer_year + r'|(present|current|till date|today))'
    date_range = r"(" + start_date + r"(" + not_alpha_numeric + r"{1,4}|(\s*to\s*))" + end_date + r")|(" + year_range + r")"

    
    regular_expression = re.compile(date_range, re.IGNORECASE)
    
    regex_result = re.search(regular_expression, work_experience)
    
    while regex_result:
        
        try:
            date_range = regex_result.group()
            # print(date_range)
            # print("*"*100)
            try:
                
                year_range_find = re.compile(year_range, re.IGNORECASE)
                year_range_find = re.search(year_range_find, date_range)
                # print("year_range_find",year_range_find.group())
                                
                # replace = re.compile(r"(" + not_alpha_numeric + r"{1,4}|(\s*to\s*))", re.IGNORECASE)
                replace = re.compile(r"((\s*to\s*)|" + not_alpha_numeric + r"{1,4})", re.IGNORECASE)
                replace = re.search(replace, year_range_find.group().strip())
                # print(replace.group())
                # print(year_range_find.group().strip().split(replace.group()))
                start_year_result, end_year_result = year_range_find.group().strip().split(replace.group())
                # print(start_year_result, end_year_result)
                # print("*"*100)
                start_year_result = int(correct_year(start_year_result))
                if (end_year_result.lower().find('present') != -1 or 
                    end_year_result.lower().find('current') != -1 or 
                    end_year_result.lower().find('till date') != -1 or 
                    end_year_result.lower().find('today') != -1): 
                    end_month = date.today().month  # current month
                    end_year_result = date.today().year  # current year
                else:
                    end_year_result = int(correct_year(end_year_result))


            except Exception as e:
                # logging.error(str(e))
                start_date_find = re.compile(start_date, re.IGNORECASE)
                start_date_find = re.search(start_date_find, date_range)

                non_alpha = re.compile(not_alpha_numeric, re.IGNORECASE)
                non_alpha_find = re.search(non_alpha, start_date_find.group().strip())

                replace = re.compile(start_date + r"(" + not_alpha_numeric + r"{1,4}|(\s*to\s*))", re.IGNORECASE)
                replace = re.search(replace, date_range)
                date_range = date_range[replace.end():]
        
                start_year_result = start_date_find.group().strip().split(non_alpha_find.group())[-1]

                start_year_result = int(correct_year(start_year_result))

                if date_range.lower().find('present') != -1 or date_range.lower().find('current') != -1:
                    end_month = date.today().month  # current month
                    end_year_result = date.today().year  # current year
                else:
                    end_date_find = re.compile(end_date, re.IGNORECASE)
                    end_date_find = re.search(end_date_find, date_range)

                    end_year_result = end_date_find.group().strip().split(non_alpha_find.group())[-1]

                    # if len(end_year_result)<2:
                    #   if int(end_year_result) > int(str(date.today().year)[-2:]):
                    #     end_year_result = str(int(str(date.today().year)[:-2]) - 1 )+end_year_result
                    #   else:
                    #     end_year_result = str(date.today().year)[:-2]+end_year_result
                    # end_year_result = int(end_year_result)
                    try:
                        end_year_result = int(correct_year(end_year_result))
                    except Exception as e:
                        end_year_result = int(re.search("\d+",correct_year(end_year_result)).group())

            if (start_year == -1) or (start_year_result <= start_year):
                start_year = start_year_result
            if (end_year == -1) or (end_year_result >= end_year):
                end_year = end_year_result

            work_experience = work_experience[regex_result.end():].strip()
            regex_result = re.search(regular_expression, work_experience)
        except Exception as e:
            work_experience = work_experience[regex_result.end():].strip()
            regex_result = re.search(regular_expression, work_experience)
        
    total_exp =  end_year - start_year

    bio = {
        'exp' : total_exp,
        'skills' : skills
    }

    return bio

# parser('Manaal_.pdf')
