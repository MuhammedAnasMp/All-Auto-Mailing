from django.http import HttpResponse

def test(request):
    return HttpResponse("Test view works!")
    


def dashboard(request):

    return HttpResponse("Test view works!")
