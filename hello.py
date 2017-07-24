import io
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
numOfPage = input.getNumPages()
page1 = input.getPage(0)
pageContent = page1.extractText()
#print(pageContent)
import re
list = ['03-01J3-I7AP','1B-NPEB-I4C6','3L-J5DF-KVPI','4K-S3HD-Y49W', 'C3-RG3D-FL4C', 'DP-1BAU-KB6L','EH-2AI3-PXOH','FY-AZPR-E836']
pattern = re.compile('|'.join(list))
iterator = pattern.finditer(pageContent)
for match in iterator:
	print(match.start(), match.group())
	
for p in range(numOfPage):
	print(p)
	
	