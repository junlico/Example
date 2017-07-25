import io
import re
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PyPDF2 import PdfFileWriter, PdfFileReader

def mark(pdf, SKU_dict):
    title = "PLEASE LEAVE THIS LABEL UNCOVERED"     #used to determine how many labels on each page
    num_page = pdf.getNumPages()
    output = PdfFileWriter()
    for p in range(num_page):
        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=letter)
        page = pdf.getPage(p)
        page_content = page.extractText()
        block = len(re.findall(title, page_content))    #num of labels
        size = len(page_content) // block;      #divide content into blocks, get each block size
        iter = re.compile('|'.join(list(SKU_dict.keys()))).finditer(page_content)

        for match in iter:
            index = match.start() // size;      #get index of label
            #coordinate
            corX = 70 + 300 * (index // 3)
            corY = 70 + 240 * (2 - index % 3)

            c.drawString(corX, corY, SKU_dict[match.group()][0])
            corY -= 12
            c.drawString(corX, corY, SKU_dict[match.group()][1])
        c.save()
        packet.seek(0)
        sid = PdfFileReader(packet)
        page.mergePage(sid.getPage(0))
        output.addPage(page)
    output.write(open('new_package.pdf', 'wb'))

if __name__ == '__main__':
    SKU_dict = {"1E-YHCZ-6SM2":["F-83", "989-393A"],"HI-LUYL-Z9Z4":["F-101", "JL11002J"],"YZ-PXLB-7JET":["F-60", "6651D"]}
    input = PdfFileReader(open('package.pdf', 'rb'))
    mark(input, SKU_dict)
