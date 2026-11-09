import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, fireEvent, render, screen } from "@testing-library/react"
import { MemoryRouter } from "react-router-dom"
import AssistantMarkdown from "@/components/assistant/assistant-markdown"

function show(text: string) {
  return render(
    <MemoryRouter>
      <AssistantMarkdown text={text} />
    </MemoryRouter>,
  ).container
}

const scrollIntoView = Element.prototype.scrollIntoView

afterEach(() => {
  cleanup()
  vi.restoreAllMocks()
  Element.prototype.scrollIntoView = scrollIntoView
})

describe("AssistantMarkdown with untrusted text", () => {
  it("shows raw HTML as text and loads no images", () => {
    const container = show(
      [
        '<img src="x" onerror="alert(1)">',
        "",
        "<script>alert(1)</script>",
        "",
        "![a plot](http://example.org/y.png) and <iframe src=\"http://example.org\"></iframe>",
      ].join("\n"),
    )
    expect(container.querySelector("img, script, iframe")).toBeNull()
    expect(container.textContent).toContain('<img src="x" onerror="alert(1)">')
    expect(container.textContent).toContain("<script>alert(1)</script>")
  })

  it("turns javascript: and data: links into plain text", () => {
    const container = show("[run me](javascript:alert(1)) and [open me](data:text/html;base64,PHNjcmlwdD4=)")
    expect(container.querySelector("a")).toBeNull()
    expect(screen.getByText("run me").tagName).toBe("SPAN")
    expect(screen.getByText("open me").tagName).toBe("SPAN")
  })

  it("opens other sites in a new tab and object pages in the app", () => {
    show("[docs](https://example.org/docs) and ZTF21abcdefg")
    const external = screen.getByRole("link", { name: "docs" })
    expect(external.getAttribute("target")).toBe("_blank")
    expect(external.getAttribute("rel")).toBe("noopener noreferrer")
    const object = screen.getByRole("link", { name: "ZTF21abcdefg" })
    expect(object.getAttribute("href")).toBe("/objects/ZTF/ZTF21abcdefg")
    expect(object.getAttribute("target")).toBeNull()
  })

  it("keeps footnote links in the panel, with ids unique to each answer", () => {
    const scrolled: string[] = []
    // jsdom has no scrollIntoView.
    Element.prototype.scrollIntoView = function (this: Element) {
      scrolled.push(this.id)
    }
    const text = "A fact.[^1]\n\n[^1]: The source."
    const first = show(text)
    const second = show(text)

    const ref = first.querySelector<HTMLAnchorElement>("sup a")!
    expect(ref.getAttribute("target")).toBeNull()
    const target = ref.getAttribute("href")!.slice(1)
    expect(first.querySelector(`[id="${target}"]`)?.textContent).toContain("The source.")
    expect(second.querySelector(`[id="${target}"]`)).toBeNull()

    const click = fireEvent.click(ref)
    expect(click).toBe(false) // default prevented: the page's URL is left alone
    expect(scrolled).toEqual([target])

    const back = first.querySelector<HTMLAnchorElement>('a[aria-label^="Back to reference"]')!
    expect(back.getAttribute("target")).toBeNull()
    expect(first.querySelector(`[id="${back.getAttribute("href")!.slice(1)}"]`)).toBe(ref)
    // The footnotes' heading stays for screen readers only.
    expect(first.querySelector("#footnote-label")?.className).toContain("sr-only")
  })

  it("puts the model's headings below the panel's title", () => {
    show("# Summary\n\n## Light curve\n\n### Details")
    expect(screen.getByRole("heading", { name: "Summary" }).tagName).toBe("H3")
    expect(screen.getByRole("heading", { name: "Light curve" }).tagName).toBe("H4")
    expect(screen.getByRole("heading", { name: "Details" }).tagName).toBe("H5")
  })
})
