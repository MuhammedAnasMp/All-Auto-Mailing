
from django.urls import path
from .views import verify_file ,stop_task ,finalize_upload ,get_all_progress ,delete_task_progress

urlpatterns = [
    path('verify_file', verify_file),
    path('stop-task/<str:task_id>/', stop_task, name='stop_task'),
    path('finalize-upload/',finalize_upload , name='stop_task'),
     path("progress/", get_all_progress, name="get_progress"),
      path("progress/delete/", delete_task_progress, name="delete_task_progress"),

]
