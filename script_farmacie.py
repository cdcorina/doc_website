import MySQLdb as mdb
import sys
import httplib2
import datetime
import pytz
from BeautifulSoup import BeautifulSoup
conn = httplib2.Http(".cache")
pageF = conn.request(u"http://www.vrisko.gr/en/pharmacy-duties/kilkis","GET")
soupF = BeautifulSoup(pageF[1])

#data azi - doar pentru verificare
mylist = []
today = datetime.date.today()
mylist.append(today)
print mylist[len(mylist)-1] # pr
data_azi=mylist[len(mylist)-1]

farmacie_nume = []
farmacie_adresa = []
farmacie_telefon = []
farmacie_orar = []
farmacie_data = []
farmacie_email = []


try:
    con = mdb.connect('mysqldjango.coportals.com', 'mysqldjango', 'Njimko123', 'mysqldjangodb')
    cur = con.cursor()
    cur.execute("SELECT VERSION()")
    ver = cur.fetchone()   
    print "Database version : %s " % ver
    
    #farmacie_nume 
    #stocat in  farmacie_nume 
    linesF_farmacie=soupF.findAll('div',{'class':'company_name'})
    for l in linesF_farmacie:
            for x in l.findAll('a'):
                farmacie_nume.append(x.text.replace('W','O'))
    
    #farmacie_adresa
    #stocat in farmacie_adresa  
    linesF_adresa=soupF.findAll('div',{'class':'address_name'})
    for l in linesF_adresa:
        farmacie_adresa.append(l.text.replace('W','O'))
    
    #farmacie_telefon
    #stocat in farmacie_telefon
    linesF_telefon=soupF.findAll('div',{'class':'phone'})
    for l in linesF_telefon:
            for x in l.findAll('span'):
                farmacie_telefon.append(x.text)
    
    #farmacie_data
    #stocat in farmacie_data
    linesF_data=soupF.findAll('div',{'class':'working_hours'})
    s1='2014'
    for l in linesF_data:
        for x in l.findAll('span'):
            print x.text[x.text.index(s1)-6:x.text.index(s1)+4]
            farmacie_data.append(x.text[x.text.index(s1)-6:x.text.index(s1)+4])
    
    #farmacie_orar
    #stocat in farmacie_orar
    linesF_orar=soupF.findAll('div',{'class':'working_hours'})
    for l in linesF_orar:
        for x in l.findAll('span'):
            farmacie_orar.append(x.text[x.text.index(s1)+5:x.text.index(s1)+18])
     
    #rezultat_farmacie
    cur.execute("SELECT concat(farmacie_nume, data) FROM garzi_farmacii")
    garzi_farmacie_data = cur.fetchall()
    i=0
    for l in linesF_farmacie:
            d,m,y = farmacie_data[i].split('/')
            data_corecta=(datetime.date(int(y),int(m),int(d)))
            if (not any((farmacie_nume[i] + str(data_corecta)) in s for s in garzi_farmacie_data)):
                #mai intai sterge ce exista; am ales current date momentan, poate data_corecta in viitor?
                sterge=("DELETE FROM garzi_farmacii WHERE data < %s")              
                data_sterge = (datetime.datetime.strftime(pytz.datetime.datetime.utcnow(),"%Y-%m-%d"))
                cur.execute(sterge, data_sterge)
                
                add_garzi_farmacie = ("INSERT INTO garzi_farmacii(data, farmacie_nume, orar, farmacie_telefon,farmacie_adresa, tara) VALUES (%s, %s, %s, %s,%s, %s)")
                data_garzi_farmacie = (data_corecta, farmacie_nume[i], farmacie_orar[i], farmacie_telefon[i], farmacie_adresa[i], 'GR')
                cur.execute(add_garzi_farmacie,data_garzi_farmacie)
                con.commit()
                garzi_farmacie = cur.fetchone()
                print "INSERTF done. Data azi: %s, Data HTML: %s" % (data_azi,farmacie_data[i])
            i=i+1
    
except mdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1) 
    
finally:    
    if con:    
        con.close()