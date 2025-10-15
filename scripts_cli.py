import asyncio
from loguru import logger
from rich.prompt import Confirm, Prompt
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from arbitrage_param import BinanceLighterArbitrageParam, HyperliquidLighterArbitrageParam, MultiExchangeArbitrageParam, \
    BinanceHyperliquidArbitrageParam
from simple_pair_position_builder import simple_pair_position_builder_cli

console = Console()


def display_exchange_selection_menu():
    """显示交易所选择菜单"""
    # 创建标题
    title = Panel(
        "[bold blue]🚀 多交易所套利系统[/bold blue]\n[green]选择交易所配置模式[/green]",
        border_style="blue"
    )
    console.print(title)

    # 创建选项表格
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("选项", style="cyan", width=8)
    table.add_column("模式", style="green")
    table.add_column("说明", style="white")

    table.add_row("1", "🌐 多交易所模式", "支持动态选择多个交易所，自由配置交易对")
    table.add_row("2", "⚡ 预设配对模式", "使用预设的交易所配对，快速开始")
    table.add_row("3", "🔧 自定义模式", "手动指定要初始化的交易所")

    console.print(table)
    console.print()


def display_available_exchanges():
    """显示可用的交易所列表"""
    # 创建临时实例来获取可用交易所
    temp_param = MultiExchangeArbitrageParam(auto_init=False)
    available_exchanges = temp_param.get_available_exchange_codes()

    table = Table(show_header=True, header_style="bold yellow")
    table.add_column("代码", style="cyan", width=12)
    table.add_column("交易所名称", style="green")
    table.add_column("状态", style="white")

    exchange_names = {
        'binance': 'Binance',
        'bybit': 'Bybit',
        'hyperliquid': 'HyperLiquid',
        'lighter': 'Lighter',
        'aster': 'Aster',
        'okx': 'OKX'
    }

    for code in available_exchanges:
        name = exchange_names.get(code, code.upper())
        table.add_row(code, name, "✅ 可用")

    console.print(Panel("[bold yellow]📋 可用交易所列表[/bold yellow]", border_style="yellow"))
    console.print(table)
    console.print()


async def select_multi_exchange_mode():
    """多交易所模式选择"""
    console.print("[bold blue]🌐 多交易所模式[/bold blue]")
    console.print("将初始化所有可用的交易所，然后可以选择交易对")
    console.print()

    if not Confirm.ask("是否继续初始化所有可用交易所?", default=True):
        return None

    # 初始化所有交易所
    arbitrage_param = MultiExchangeArbitrageParam()

    console.print(f"[green]✅ 成功初始化 {len(arbitrage_param.exchanges)} 个交易所[/green]")

    if len(arbitrage_param.exchanges) < 2:
        console.print("[red]❌ 可用交易所少于2个，无法进行套利[/red]")
        return None

    # 显示交易所配对选择
    return await select_exchange_pair(arbitrage_param)


async def select_exchange_pair(arbitrage_param):
    """选择交易所配对"""
    initialized_codes = arbitrage_param.get_initialized_exchange_codes()

    if len(initialized_codes) == 2:
        # 只有两个交易所，自动设置为配对
        ex1, ex2 = initialized_codes[0], initialized_codes[1]
        arbitrage_param.set_exchange_pair(ex1, ex2)
        console.print(f"[green]✅ 自动设置交易所配对: {ex1} <-> {ex2}[/green]")
        return arbitrage_param

    # 多个交易所，让用户选择
    console.print("\n[bold yellow]📊 选择交易所配对[/bold yellow]")

    # 选择交易所1
    exchange1 = Prompt.ask(
        "选择第一个交易所",
        choices=initialized_codes,
        default=initialized_codes[0]
    )

    # 选择交易所2
    remaining_choices = [code for code in initialized_codes if code != exchange1]
    exchange2 = Prompt.ask(
        "选择第二个交易所",
        choices=remaining_choices,
        default=remaining_choices[0]
    )

    arbitrage_param.set_exchange_pair(exchange1, exchange2)
    console.print(f"[green]✅ 已设置交易所配对: {exchange1} <-> {exchange2}[/green]")

    return arbitrage_param


async def select_preset_pair_mode():
    """预设配对模式选择"""
    console.print("[bold green]⚡ 预设配对模式[/bold green]")

    preset_pairs = [
        ("1", "HyperLiquid + Lighter", "hyperliquid_lighter"),
        ("2", "Binance + Lighter", "binance_lighter"),
        ("3", "Binance + HyperLiquid", "binance_hyperliquid"),
    ]

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("选项", style="yellow", width=6)
    table.add_column("交易所配对", style="green")
    table.add_column("代码", style="white")

    for option, name, code in preset_pairs:
        table.add_row(option, name, code)

    console.print(table)

    choice = Prompt.ask(
        "选择预设配对",
        choices=["1", "2", "3"],
        default="2"
    )

    # 根据选择创建对应的参数类
    if choice == "1":
        return HyperliquidLighterArbitrageParam()
    elif choice == "2":
        return BinanceLighterArbitrageParam()
    elif choice == "3":
        return BinanceHyperliquidArbitrageParam()


async def select_custom_mode():
    """自定义模式选择"""
    console.print("[bold magenta]🔧 自定义模式[/bold magenta]")

    # 显示可用交易所
    display_available_exchanges()

    # 获取可用交易所列表
    temp_param = MultiExchangeArbitrageParam(auto_init=False)
    available_exchanges = temp_param.get_available_exchange_codes()

    # 让用户选择要初始化的交易所
    console.print("\n[bold yellow]请选择要初始化的交易所 (可多选，用空格分隔):[/bold yellow]")

    selected_exchanges = []
    for i, exchange in enumerate(available_exchanges, 1):
        if Confirm.ask(f"是否初始化 {exchange.upper()}?", default=True):
            selected_exchanges.append(exchange)

    if len(selected_exchanges) < 2:
        console.print("[red]❌ 至少需要选择2个交易所[/red]")
        return None

    console.print(f"[green]✅ 已选择交易所: {', '.join(selected_exchanges)}[/green]")

    # 初始化选中的交易所
    arbitrage_param = MultiExchangeArbitrageParam(exchange_codes=selected_exchanges)

    # 选择交易所配对
    return await select_exchange_pair(arbitrage_param)


async def main():
    """主函数"""
    while True:
        try:
            console.clear()
            display_exchange_selection_menu()

            mode = Prompt.ask(
                "请选择模式",
                choices=["1", "2", "3", "exit"],
                default="2"
            )

            if mode == "exit":
                console.print("[yellow]👋 再见![/yellow]")
                break

            console.print()

            arbitrage_param = None

            if mode == "1":
                arbitrage_param = await select_multi_exchange_mode()
            elif mode == "2":
                arbitrage_param = await select_preset_pair_mode()
            elif mode == "3":
                arbitrage_param = await select_custom_mode()

            if arbitrage_param is None:
                console.print("[red]❌ 参数创建失败，请重试[/red]")
                if not Confirm.ask("是否重新选择?", default=True):
                    continue
                else:
                    continue

            # 显示选择的配置信息
            if hasattr(arbitrage_param, 'get_exchange_pair_status'):
                status = arbitrage_param.get_exchange_pair_status()
                if status['current_pair']:
                    pair = status['current_pair']
                    console.print(Panel(
                        f"[bold green]✅ 当前配置[/bold green]\n"
                        f"交易所1: {pair['exchange1'].upper()}\n"
                        f"交易所2: {pair['exchange2'].upper()}\n"
                        f"总交易所数: {status['total_exchanges']}",
                        border_style="green"
                    ))

            # 询问是否继续
            if not Confirm.ask("是否继续进行交易配置?", default=True):
                continue

            # 初始化异步适配器
            try:
                await arbitrage_param.init_async_exchanges()
                logger.info("✅ 异步交易所适配器初始化完成")
            except Exception as e:
                logger.error(f"❌ 异步适配器初始化失败: {e}")
                # 降级到同步模式
                if hasattr(arbitrage_param, 'exchange2') and arbitrage_param.exchange2:
                    if hasattr(arbitrage_param.exchange2, 'init'):
                        await arbitrage_param.exchange2.init()

            # 进入交易配置界面
            await simple_pair_position_builder_cli(arbitrage_param)
            await arbitrage_param.close_async_exchanges()

        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ 用户手动中断操作[/yellow]")
            if not Confirm.ask("是否重新开始?", default=True):
                break
        except Exception as e:
            console.print(f"[red]❌ 程序异常: {e}[/red]")
            logger.exception(e)
            if not Confirm.ask("是否重新开始?", default=True):
                break


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]👋 用户手动退出程序[/yellow]")
    except Exception as e:
        console.print(f"[red]❌ 程序异常退出: {e}[/red]")
        logger.exception(e)

