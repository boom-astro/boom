import posthog, { type Properties } from 'posthog-js';

function event<P extends Properties>(name: string) {
  return (properties?: P) => posthog.capture(name, properties);
}

export const trackSignupInitiated = () => posthog.capture('signup_initiated');
export const trackSignupEmailSubmitted = () => posthog.capture('signup_email_submitted');
export const trackActivationCodeSubmitted = () => posthog.capture('activation_code_submitted');
export const trackAccountActivated = event<{ via_link?: boolean }>('account_activated');
export const trackLoginSuccess = () => posthog.capture('login_success');

export const trackKafkaCredentialCreateInitiated = event<{ credential_name?: string }>('kafka_credential_create_initiated');
export const trackKafkaCredentialCreated = event<{ credential_id?: string; credential_name?: string }>('kafka_credential_created');
export const trackKafkaCredentialDeleted = event<{ credential_id?: string }>('kafka_credential_deleted');
export const trackCredentialSecretToggled = event<{ credential_id?: string; revealed?: boolean }>('credential_secret_toggled');
export const trackCredentialCopied = event<{ label?: string }>('credential_copied');

export const trackApiTokenCreateInitiated = event<{ token_name?: string; expiry_days?: number }>('api_token_create_initiated');
export const trackApiTokenCreated = event<{ token_id?: string; token_name?: string; expiry_days?: number }>('api_token_created');
export const trackApiTokenDeleted = event<{ token_id?: string }>('api_token_deleted');

export const trackAlertSearchSubmitted = event<Properties>('alert_search_submitted');
export const trackAlertSearchCompleted = event<Properties>('alert_search_completed');
export const trackObjectSearchSubmitted = event<Properties>('object_search_submitted');
export const trackObjectSearchCompleted = event<Properties>('object_search_completed');

export function trackError(context: string, error: unknown, additionalInfo?: Properties) {
  posthog.capture('error_occurred', {
    category: 'error',
    context,
    error_message: error instanceof Error ? error.message : String(error),
    ...additionalInfo,
  });
}

export const identifyUser = (userId: string) => posthog.identify(userId);

export const resetUser = () => posthog.reset();
