import os
import urllib.request
import uno
from flask import Flask
from flask import send_file
app = Flask(__name__)

def generate_xls_with_loaded_cache():
    url = 'http://www.anp.gov.br/arquivos/dados-estatisticos/vendas-combustiveis/vendas-combustiveis-m3.xls'
    file_name, headers = urllib.request.urlretrieve(url)
    local_ctx = uno.getComponentContext()
    smgr_local = local_ctx.ServiceManager
    resolver = smgr_local.createInstanceWithContext("com.sun.star.bridge.UnoUrlResolver", local_ctx)
    url = "uno:socket,host=libreoffice,port=8100,tcpNoDelay=1;urp;StarOffice.ComponentContext"
    uno_ctx = resolver.resolve(url)
    uno_smgr = uno_ctx.ServiceManager
    desktop = uno_smgr.createInstanceWithContext("com.sun.star.frame.Desktop", uno_ctx )
    PropertyValue = uno.getClass('com.sun.star.beans.PropertyValue')
    inProps = PropertyValue( "Hidden" , 0 , True, 0 ), 
    document = desktop.loadComponentFromURL(uno.systemPathToFileUrl(file_name), "_blank", 0, inProps )

    path = os.path.join('/tmp/', 'vendas-combustiveis-m3-loaded-cache.xls')
    uno_url = uno.systemPathToFileUrl(path)
    filter = uno.createUnoStruct('com.sun.star.beans.PropertyValue')
    filter.Name = 'FilterName'
    filter.Value = 'Calc MS Excel 2007 XML'
    filters = (filter,)

    document.storeToURL(uno_url, filters)

@app.route('/download')
def downloadFile ():
    generate_xls_with_loaded_cache()
    path = "/tmp/vendas-combustiveis-m3-loaded-cache.xls"
    return send_file(path, as_attachment=True)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
