import re

class url():

    def __init__(self,job_name,location_name):
        self.job_name = re.sub('[^a-zA-Z]+',' ',job_name)
        self.location_name = re.sub('[^a-zA-Z]+',' ',location_name)

    def linkdin_url(self):
        position = ""
        for name in self.job_name.split(" "):
            if name != self.job_name.split(" ")[-1]:
                position += name + "%20"
            else:
                position += name 

        place = ""
        for name in self.location_name.split(" "):
            if name != self.location_name.split(" ")[-1]:
                place += name + "%20"
            else:
                place += name

        link = f"https://www.linkedin.com/jobs/search?keywords={position}&location={place}&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&distance=75"
        return link

    def naukri_url(self):
        position = ""
        for name in self.job_name.split(" "):
            if name != self.job_name.split(" ")[-1]:
                position += name + "-"
            else:
                position += name 

        place = ""
        for name in self.location_name.split(" "):
            if name != self.location_name.split(" ")[-1]:
                place += name + "-"
            else:
                place += name

        link = f"https://www.naukri.com/{position}-jobs-in-{place}"
        return link

    def indeed_url(self):
        position = ""
        for name in self.job_name.split(" "):
            if name != self.job_name.split(" ")[-1]:
                position += name + "+"
            else:
                position += name 

        place = ""
        for name in self.location_name.split(" "):
            if name != self.location_name.split(" ")[-1]:
                place += name + "%2C+"
            else:
                place += name

        link = f"https://in.indeed.com/jobs?q={position}&l={place}"
        return link

    def shine_url(self):
        position = ""
        position_lower = ""
        for name in self.job_name.split(" "):
            if name != self.job_name.split(" ")[-1]:
                position_lower += name.lower() + '-'
                position += name + "+"
            else:
                position_lower += name
                position += name 

        place = ""
        place_lower = ""
        for name in self.location_name.split(" "):
            if name != self.location_name.split(" ")[-1]:
                place_lower += name + '-'
                place += name + "%2C+"
            else:
                place_lower += name
                place += name

        link = f"https://www.shine.com/job-search/{position_lower}-jobs-in-{place_lower}?q={position}%2C&loc={place}"
        return link

# u = url('Project Managment',"Canada")
# u.linkdin_url()