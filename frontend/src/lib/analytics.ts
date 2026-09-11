import posthog, { type Properties } from 'posthog-js';
import type { Profile } from '@/lib/api';

type SearchProps = Record<string, unknown>;

function event<P extends Properties>(name: string) {
  return (properties?: P) => posthog.capture(name, properties);
}

/** Takes no properties, so an address cannot be attached to it by accident. */
function bareEvent(name: string) {
  return () => posthog.capture(name);
}

export const trackSignupInitiated = bareEvent('signup_initiated');
export const trackSignupEmailSubmitted = bareEvent('signup_email_submitted');
export const trackActivationCodeSubmitted = bareEvent('activation_code_submitted');
export const trackAccountActivated = event<{ via_link?: boolean }>('account_activated');
export const trackLoginSuccess = bareEvent('login_success');

export const trackKafkaCredentialCreateInitiated = event<{ credential_name?: string }>('kafka_credential_create_initiated');
export const trackKafkaCredentialCreated = event<{ credential_id?: string; credential_name?: string }>('kafka_credential_created');
export const trackKafkaCredentialDeleted = event<{ credential_id?: string }>('kafka_credential_deleted');
export const trackCredentialSecretToggled = event<{ credential_id?: string; revealed?: boolean }>('credential_secret_toggled');
export const trackCredentialCopied = event<{ label?: string }>('credential_copied');

export const trackApiTokenCreateInitiated = event<{ token_name?: string; expiry_days?: number }>('api_token_create_initiated');
export const trackApiTokenCreated = event<{ token_id?: string; token_name?: string; expiry_days?: number }>('api_token_created');
export const trackApiTokenDeleted = event<{ token_id?: string }>('api_token_deleted');

export const trackAlertSearchSubmitted = event<SearchProps>('alert_search_submitted');
export const trackAlertSearchCompleted = event<SearchProps>('alert_search_completed');
export const trackObjectSearchSubmitted = event<SearchProps>('object_search_submitted');
export const trackObjectSearchCompleted = event<SearchProps>('object_search_completed');

export function trackError(context: string, error: unknown, additionalInfo?: SearchProps) {
  posthog.capture('error_occurred', {
    category: 'error',
    context,
    error_message: error instanceof Error ? error.message : String(error),
    ...additionalInfo,
  });
}

export function identifyUser(userId: string, email?: string, username?: string) {
  const previousId = posthog.get_distinct_id();
  posthog.identify(userId);
  // Alias after identify: identify skips its distinct_id switch when handed the registered __alias.
  const vouchedFor = !!previousId && (previousId === username || previousId === email);
  if (vouchedFor && previousId !== userId) {
    posthog.alias(userId, previousId);
  }
}

export function identifyProfile(profile: NonNullable<Profile>) {
  identifyUser(profile.id ?? profile.username, profile.email, profile.username);
}

export function resetUser() {
  posthog.reset();
}
