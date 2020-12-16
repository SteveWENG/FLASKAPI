# -*- coding: utf-8 -*-

from io import BytesIO
import qrcode

def QRCode(code):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=1,
    )
    qr.add_data(code)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")
    return img

def QRCodeBytes(code):
    img = QRCode(code)
    out = BytesIO()
    img.save(out,'PNG')

    return out.getvalue()