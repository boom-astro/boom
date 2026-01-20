// Dynamic loader for Aladin Lite library
// Loads the script only when needed, not blocking initial page load

let aladinLoadPromise: Promise<void> | null = null;

export function loadAladinScript(): Promise<void> {
  // If already loaded or loading, return existing promise
  if (aladinLoadPromise) {
    return aladinLoadPromise;
  }

  // If already loaded in window
  if (window.A) {
    return Promise.resolve();
  }

  // Start loading
  aladinLoadPromise = new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.type = 'text/javascript';
    script.src = 'https://aladin.cds.unistra.fr/AladinLite/api/v3/latest/aladin.js';
    script.async = true; // Load asynchronously
    script.onload = () => {
      // Wait for Aladin internal initialization
      if (window.A && window.A.init) {
        window.A.init.then(() => resolve()).catch(reject);
      } else {
        reject(new Error('Aladin A.init not available'));
      }
    };
    script.onerror = () => {
      aladinLoadPromise = null; // Reset so it can be retried
      reject(new Error('Failed to load Aladin Lite library'));
    };

    document.head.appendChild(script);
  });

  return aladinLoadPromise;
}
