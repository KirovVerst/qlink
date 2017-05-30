import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATABASE = {
    'default': {
        "dialect": "sqlite",
        "db_name": "data/db/example.sqlite3",
        "query": "SELECT * FROM dataset",
        "user": "",
        "password": "",
        "host": "",  # example : localhost:3306
    },
    "university": {
        "sql": ""
    }
}

"""
CREATE VIEW PersonalDataView
	AS 
		Select p.Oid PersonOid, p.FirstName, p.SecondName, p.MiddleName, p.FirstNameEng, p.SecondNameEng, p.MiddleNameEng,
		p.Sex, p.BirthDate, p.PlaceBirth, p.Login, p.Email, p.Phone,
		pc.Name Country,
		cr.Name Region, cr.Code RegionCode, Address,
		bd.Name DocumentName, bd.Series DocumentSeries, bd.Number DocumentNumber, bd.Date DocumentDate, pd.GivenOrganization DocumentGivenOrganization,
		y.Name EntranceYear, d.Name Degree, f.Name Faculty,  s.Name Speciality, sf.Name StudyForm, sb.Name StudyBasis
		from Person p
		join CountryRegion cr on p.CountryRegion = cr.Oid
		join PersonCountry pc on pc.Oid = p.PersonCountry
		join Student st on st.Person = p.Oid
		join Year y on st.EntranceYear = y.Oid
		join Faculty f on st.Faculty = f.Oid
		join Degree d on st.Degree = d.Oid
		join Speciality s on st.Speciality = s.Oid
		join StudyBasis sb on st.StudyBasis = sb.Oid
		join StudyForm sf on st.StudyForm = sf.Oid
		join PersonDocument pd on pd.Person = p.Oid
		join BaseDocument bd on pd.Oid = bd.Oid
"""
