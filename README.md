# Steps to load the webvisual page

## 1. Open Terminal and type "python -m SimpleHTTPServer 1337"

1337 is the port number I created to present main.html

## 2. Open "main.html" using any script editor like SublimeText, modify all file paths according to the address that you store the "WebVisual" folder at. 

For instance, `<link rel="stylesheet" type="text/css" href="http://localhost:1337/Documents/TwitterCode/WebVisual/lib/css/bootstrap.min.css">` is the html code to load "bootstrap.min.css" package. You should change "/Documents/TwitterCode/" as needed. 

## 3. Open up any webbrowser and type `http://localhost:1337/YOUR PATH/WebVisual/main.html`