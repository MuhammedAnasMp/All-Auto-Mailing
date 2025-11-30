
from django.urls import path
from .views import test ,dashboard

urlpatterns = [
    path('', dashboard),

    
    path('test/', test),
]
