#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import os
import re
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PyPDF2 import PdfFileWriter, PdfFileReader
#import getData

def box_label(file_dir, SKU_dict):
    pdf = PdfFileReader(open(file_dir, 'rb'))
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

        c.drawString(65, 775, "Box Label")
        for match in iter:
            index = match.start() // size;      #get index of label
            #coordinate
            corX = 60 + 300 * (index // 3)
            corY = 60 + 240 * (2 - index % 3)

            c.drawString(corX, corY, SKU_dict[match.group()][0])
            corY -= 12
            c.drawString(corX, corY, SKU_dict[match.group()][1])
        c.save()
        packet.seek(0)
        sid = PdfFileReader(packet)
        page.mergePage(sid.getPage(0))
        output.addPage(page)
    output.write(open(file_dir[:-4]+"_new.pdf", 'wb'))

def pallet_label(file_dir):
    pdf = PdfFileReader(open(file_dir, 'rb'))
    num_page = pdf.getNumPages()
    output = PdfFileWriter()
    for p in range(num_page):
        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=letter)
        page = pdf.getPage(p)
        c.drawString(65, 775, "Pallet Label")
        c.save()
        packet.seek(0)
        sid = PdfFileReader(packet)
        page.mergePage(sid.getPage(0))
        output.addPage(page)
    output.write(open(file_dir[:-4]+"_new.pdf", 'wb'))

def ups_label(file_dir, SKU_dict):
    pdf = PdfFileReader(open(file_dir, 'rb'))
    num_page = pdf.getNumPages()
    output = PdfFileWriter()

    for p in range(num_page):
        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=letter)
        page = pdf.getPage(p)
        page_content = page.extractText()
        iter = re.compile('|'.join(list(SKU_dict.keys()))).finditer(page_content)
        corX = 500
        corY = -265
        for match in iter:
            c.saveState()
            c.rotate(90)
            c.drawString(corX, corY, SKU_dict[match.group()][0])
            corY -= 15
            c.drawString(corX, corY, SKU_dict[match.group()][1])
        c.restoreState()
        c.save()
        packet.seek(0)
        sid = PdfFileReader(packet)
        page.mergePage(sid.getPage(0))
        output.addPage(page)
    output.write(open(file_dir[:-4]+"_new.pdf", 'wb'))

def getPDFFile_list(SKU_dict):
    download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
    file_list = os.listdir(download_dir)
    pallet = re.compile("pallet")
    for file_name in file_list:
        if file_name.endswith(".pdf"):
            file_dir = os.path.join(download_dir, file_name)
            if pallet.match(file_name):
                pallet_label(file_dir)
            else:
                ups_label(file_dir, SKU_dict)



if __name__ == '__main__':
    SKU_dict ={"WX-JG7P-4JP1":["F-120","2231-1"]}
    getPDFFile_list(SKU_dict)
    #box_label(file_dir, SKU_dict)
