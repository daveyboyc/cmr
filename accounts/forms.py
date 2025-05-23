from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User

class UserRegistrationForm(UserCreationForm):
    email = forms.EmailField(required=True, help_text='Required. Will be used for login.')

    class Meta(UserCreationForm.Meta):
        model = User
        # Use email and password fields. Username is excluded from the form.
        fields = ('email',)

    def save(self, commit=True):
        # Set the username to the email before saving
        user = super().save(commit=False)
        user.username = self.cleaned_data["email"]
        if commit:
            user.save()
        return user 