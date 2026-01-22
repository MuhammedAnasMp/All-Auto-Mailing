
from django.urls import path
from .views import verify_file ,stop_task

urlpatterns = [
    path('verify_file', verify_file),
    path('stop-task/<str:task_id>/', stop_task, name='stop_task'),

]
