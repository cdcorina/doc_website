import MySQLdb as mdb
import sys
import httplib2
import datetime
import pytz
from BeautifulSoup import BeautifulSoup
conn = httplib2.Http(".cache")
page = conn.request(u"http://www.vrisko.gr/en/hospital-duties/thessaloniki/all-clinics","GET")
soup = BeautifulSoup(page[1])

#lines=soup.findAll('div','block4fr')
#len(lines)
#lines=soup.findAll('div','block text')
#lines=[x for x in lines if x.findChildren()][:-1]
#lines = [x for x in lines if x['id']=='block4fr']
#rows=[x for x in lines[0].findAll('tr')]  
#for r in rows:
# print r.findChildren()
#data=[x.findChildren()[0] for x in rows if x.findChildren()[0]]
#spital=[x.findChildren()[1] for x in rows if x.findChildren()[1]]
#final=zip(data,spital)
#for r in rows:
# if r.findChildren()[1] != "<br />":
#     print r.findChildren()[1]
# else:
#     print r.findChildren()[2]
 

#data azi - doar pentru verificare
mylist = []
today = datetime.date.today()
mylist.append(today)
print mylist[len(mylist)-1] # pr
data_azi=mylist[len(mylist)-1]

spital_nume = []
spital_adresa = []
spital_telefon = []
spital_orar = []
spital_data = []
spital_specializare = []

try:
    con = mdb.connect('mysqldjango.coportals.com', 'mysqldjango', 'Njimko123', 'mysqldjangodb')
    cur = con.cursor()
    cur.execute("SELECT VERSION()")
    ver = cur.fetchone()   
    print "Database version : %s " % ver
    
    #spital_nume 
    #stocat in spital_nume 
    lines_spitale=soup.findAll('div',{'class':'company_name'})
    #for l in lines:
    # rows=l.findAll('tr')
    # for r in rows:
    #    print r.text
    for l in lines_spitale:
            for x in l.findAll('a'):
                spital_nume.append(x.text.replace('W','O'))
    
    #spital_adresa
    #stocat in spital_adresa  
    lines_adresa=soup.findAll('div',{'class':'address_name'})
    for l in lines_adresa:
            spital_adresa.append(l.text.replace('W','O'))
            
    #spital_specializare
    #stocat in spital_specializare
    lines_spec=soup.findAll('div',{'class':'clinicsTitle'})
    for l in lines_spec:
            spital_specializare.append(l.text)
                
    #spital_telefon
    #stocat in spital_telefon
    lines_telefon=soup.findAll('div',{'class':'phone'})
    for l in lines_telefon:
            for x in l.findAll('span'):
                spital_telefon.append(x.text)
    
    #spital_data
    #stocat in spital_data
    lines_data=soup.findAll('div',{'class':'date'})
    s1='2014'
    for l in lines_data:
            spital_data.append(l.text[l.text.index(s1)-6:l.text.index(s1)+4])
            
    #spital_orar
    #stocat in spital_orar
    lines_orar=soup.findAll('div',{'class':'clinicsHourRange'})
    for l in lines_orar:
            spital_orar.append(l.text)   
    
    #rezultat
    cur.execute("SELECT concat(spital_nume, data) FROM garzi")
    garzi_spitale_data = cur.fetchall()
    i=0
    for l in lines_spitale:
        d,m,y = spital_data[i].split("/")
        data_corecta = (datetime.date(int(y),int(m), int(d)))
        if (not any( (spital_nume[i] + str(data_corecta)) in s for s in garzi_spitale_data) ):
            #mai intai sterge ce exista am ales current date momentan, poate data_corecta in viitor?
            sterge=("DELETE FROM garzi WHERE data < %s")
            data_sterge = (datetime.datetime.strftime(pytz.datetime.datetime.utcnow(),"%Y-%m-%d"))
            cur.execute(sterge, data_sterge)
            
            add_garzi = ("INSERT INTO garzi(data, spital_id, sectie_nume, orar, spital_nume, spital_telefon, spital_adresa, tara) VALUES (%s, %s, %s, %s,%s, %s, %s, %s)")
            data_garzi = (data_corecta, 1 , spital_specializare[i], spital_orar[i], spital_nume[i], spital_telefon[i], spital_adresa[i], 'GR')
            cur.execute(add_garzi, data_garzi)
            con.commit()
            garzi = cur.fetchone()
            print "INSERT done. Data azi: %s, Data HTML: %s" % (data_azi,spital_data[i])
        i=i+1
    
except mdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1) 
    
finally:    
    if con:    
        con.close()