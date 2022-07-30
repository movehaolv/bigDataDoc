from urllib.request import urlopen

resp = urlopen("http://www.baidu.com")

print(resp.read().decode("utf-8"))