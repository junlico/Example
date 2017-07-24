import io
import re
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PyPDF2 import PdfFileWriter, PdfFileReader

'''
packet = io.BytesIO()
c = canvas.Canvas(packet, pagesize=letter)
width, height = letter
print(str(width) + " " + str(height))
c.drawString(80, 550, '1111111111')
c.drawString(80, 310, '2222222222')
c.drawString(80, 70, '3333333333')
c.drawString(380, 550, '4444444444')
c.drawString(380, 310, '5555555555')
c.drawString(380, 70, '6666666666')
c.save()
packet.seek(0)

sid = PdfFileReader(packet)

output = PdfFileWriter()

package = PdfFileReader(open('package.pdf', 'rb'))

page1 = package.getPage(0)
page1.mergePage(sid.getPage(0))
output.addPage(page1)

outputStream = open('new.pdf','wb')
output.write(outputStream)
'''

input = PdfFileReader(open('package.pdf', 'rb'))
output = PdfFileWriter()
numOfPage = input.getNumPages()
'''	
page1 = input.getPage(0)
pageContent = page1.extractText()

list = ['03-01J3-I7AP','1B-NPEB-I4C6','3L-J5DF-KVPI','4K-S3HD-Y49W', 'C3-RG3D-FL4C', 'DP-1BAU-KB6L','EH-2AI3-PXOH','FY-AZPR-E836',
		'Y9-5CTB-8KB6','WZ-RSJX-N54E','VG-MJJ7-GG58','RF-M8HT-PM7D','LC-5NMN-YFXI','JT-PJRM-QJXI','I1-0KWM-Y30T'
		]
		
pattern = re.compile('|'.join(list))
iterator = pattern.finditer(pageContent)
block = len(pageContent) / 6
i = 0
for match in iterator:
	print(match.start(), match.start() // block, match.group())
	i += 1
'''
block = 6;
list = ['03-01J3-I7AP','1B-NPEB-I4C6','3L-J5DF-KVPI','4K-S3HD-Y49W', 'C3-RG3D-FL4C', 'DP-1BAU-KB6L','EH-2AI3-PXOH','FY-AZPR-E836',
		'Y9-5CTB-8KB6','WZ-RSJX-N54E','VG-MJJ7-GG58','RF-M8HT-PM7D','LC-5NMN-YFXI','JT-PJRM-QJXI','I1-0KWM-Y30T'
		]
for p in range(numOfPage):
	packet = io.BytesIO()
	c = canvas.Canvas(packet, pagesize=letter)
	
	page = input.getPage(p)
	pageContent = page.extractText()
	size = len(pageContent) / block;
	iter = re.compile('|'.join(list)).finditer(pageContent)
	
	for match in iter:
		index = match.start() // size;
		corX = 70 + 300 * (index // 3)
		corY = 70 + 240 * (2 - index % 3)
		c.drawString(corX, corY, match.group())
		corY -= 10
		c.drawString(corX, corY, match.group())
	c.save()
	packet.seek(0)
	sid = PdfFileReader(packet)
	page.mergePage(sid.getPage(0))
	output.addPage(page)
output.write(open('new1.pdf', 'wb'))

	
	