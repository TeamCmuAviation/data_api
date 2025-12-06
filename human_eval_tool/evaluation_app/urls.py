from django.urls import path
from .views import LoginView, EvaluationInterfaceView

urlpatterns = [
    path('', LoginView.as_view(), name='login'),
    path('evaluate/', EvaluationInterfaceView.as_view(), name='evaluate'),
]
