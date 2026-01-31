
from django.urls import path
from .views import verify_file ,stop_task ,finalize_upload

urlpatterns = [
    path('verify_file', verify_file),
    path('stop-task/<str:task_id>/', stop_task, name='stop_task'),
    path('finalize-upload/',finalize_upload , name='stop_task'),

]
